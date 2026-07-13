const amqp = require('amqplib/callback_api');
const acme = require('acme-client');
const { MongoClient } = require('mongodb');


(async () => {
    const path = require('path');
    const { spawn } = require('child_process');

    // acme-client shares one axios instance with no default timeout, so a hung
    // TCP connection to Let's Encrypt never settles and client.auto() waits
    // forever (the cert job stalls right after logging the CSR). Give every ACME
    // HTTP request a hard timeout so a stuck connection rejects instead of hanging.
    acme.axios.defaults.timeout = 30000;

    // Reject a promise if it hasn't settled within ms. Used as a backstop around
    // the whole ACME flow (account reg + order + dns-01 validation + finalize),
    // since the per-request timeout above only covers individual HTTP calls.
    const withTimeout = (promise, ms, label) => Promise.race([
        promise,
        new Promise((_, reject) => setTimeout(() => reject(new Error((label || 'operation') + ' timed out after ' + ms + 'ms')), ms))
    ]);
    
    // frameMax=0 keeps the broker's offered frame size (131072). amqplib otherwise
    // shrinks it to its 4096 default, which this broker rejects mid-handshake with
    // an ECONNRESET right after Connection.Open (pika never shrinks it, so it works).
    const REQUEST_QUEUE_URL = 'amqp://api.homegames.io:5672?frameMax=0';//52.32.110.71';//process.env.QUEUE_URL;
    const QUEUE_NAME = 'homegames-jobs';//process.env.QUEUE_NAME;

    // Heartbeat interval (seconds). The old value of 1 was pathological: cert jobs
    // run for minutes and any ~2s event-loop stall made the broker declare the
    // connection dead, drop it mid-job, and requeue the unacked message for
    // redelivery (observed as redelivered=true). 30s tolerates normal jitter.
    const HEARTBEAT_SECONDS = 30;
    
    // This worker handles exactly two job types off the unified homegames-jobs
    // queue: ACME certificate generation, and LLM "modify my game" requests.
    const CERT_REQUEST = 'CERT_REQUEST';
    const LLM_REQUEST = 'LLM_REQUEST';

    let running = false;

    const API_URL = process.env.API_URL;

    // --- LLM model server -------------------------------------------------
    // LLM generation runs in a long-lived Python child (llm/model_server.py)
    // that keeps the MLX model warm. Node owns the queue + result-posting; the
    // child only generates. See handleLlmRequest / ensureModelServer below.
    const LLM_WORKER_SECRET = process.env.LLM_WORKER_SECRET || '';
    const LLM_PYTHON = process.env.LLM_PYTHON || path.join(__dirname, 'llm', 'env', 'bin', 'python');
    const LLM_SERVER_PATH = process.env.LLM_SERVER_PATH || path.join(__dirname, 'llm', 'model_server.py');

    // The authoring guide that grounds the model is the single source of truth in
    // homegames-common. Resolve its path and hand it to the Python model server
    // via AUTHORING_DOC_PATH, so there's no per-repo copy to drift out of date.
    let AUTHORING_DOC_PATH = process.env.AUTHORING_DOC_PATH;
    if (!AUTHORING_DOC_PATH) {
        try {
            AUTHORING_DOC_PATH = path.join(path.dirname(require.resolve('homegames-common')), 'docs', 'squishjs-game-authoring.md');
        } catch (err) {
            console.warn('[llm] homegames-common not resolvable; set AUTHORING_DOC_PATH to ground the model with the authoring guide');
        }
    }

    const DB_NAME = process.env.DB_NAME;
    const DB_HOST = process.env.DB_HOST;
    const DB_PORT = process.env.DB_PORT
    const DB_USERNAME = process.env.DB_USERNAME || '';
    const DB_PASSWORD = process.env.DB_PASSWORD || '';
    
    const AWS_ROUTE_53_HOSTED_ZONE_ID = process.env.AWS_ROUTE_53_HOSTED_ZONE_ID;
    
    const getMongoClient = () => {
        const uri = DB_USERNAME ? `mongodb://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}` : `mongodb://${DB_HOST}:${DB_PORT}/${DB_NAME}`;
        console.log("URI");
        console.log(uri);
        const params = {};
        if (DB_USERNAME) {
            params.auth = {
                username: DB_USERNAME,
                password: DB_PASSWORD
            };
            params.authSource = 'admin';
        }
    
        return new MongoClient(uri, params);
    };
    
    const getMongoCollection = (collectionName) => new Promise((resolve, reject) => {
        const client = getMongoClient();
        client.connect().then(() => {
            const db = client.db('homegames');
            const collection = db.collection(collectionName);
            resolve(collection);
        });
    });
    
    
    const challengeCreateFn = async(authz, challenge, keyAuthorization) => {
        if (challenge.type === 'dns-01') {
            console.log('creating!!');
            await createDnsRecord(`_acme-challenge.${authz.identifier.value}`, keyAuthorization);
        }
    };
    
    const challengeRemoveFn = async(authz, challenge, keyAuthorization) => {
    
        if (challenge.type === 'dns-01') {
            console.log('removing!!');
            await deleteDnsRecord(`_acme-challenge.${authz.identifier.value}`);
        }
    };
    
    const createDnsRecord = (name, value) => new Promise((resolve, reject) => {
        const dnsParams = {
            ChangeBatch: {
                Changes: [
                    {
                        Action: 'CREATE',
                        ResourceRecordSet: {
                            Name: name,
                            ResourceRecords: [
                                {
                                    Value: '"' + value + '"'
                                }
                            ],
                            TTL: 300,
                            Type: 'TXT'
                        }
                    }
                ]
            },
            HostedZoneId: AWS_ROUTE_53_HOSTED_ZONE_ID
        };
    
        const aws = require('aws-sdk');
        const route53 = new aws.Route53();
        route53.changeResourceRecordSets(dnsParams, (err, data) => {
            if (err) {
                reject(err);
            } else {
                const params = {
                    Id: data.ChangeInfo.Id
                };
    
                route53.waitFor('resourceRecordSetsChanged', params, (waitErr, waitData) => {
                    if (waitErr) {
                        reject(waitErr);
                    } else if (waitData && waitData.ChangeInfo && waitData.ChangeInfo.Status === 'INSYNC') {
                        resolve();
                    } else {
                        reject(new Error('Route53 CREATE did not reach INSYNC: ' + JSON.stringify(waitData && waitData.ChangeInfo)));
                    }
                });
            }
        });
    });

    const deleteDnsRecord = (name) => new Promise((resolve, reject) => {
    
        getDnsRecord(name).then((value) => {
            const deleteDnsParams = {
                ChangeBatch: {
                    Changes: [
                        {
                            Action: 'DELETE',
                            ResourceRecordSet: {
                                Name: name,
                                Type: 'TXT',
                                TTL: 300,
                                ResourceRecords: [
                                    {
                                        Value: value,
                                    }
                                ]
                            }
                        }
                    ]
                },
                HostedZoneId: AWS_ROUTE_53_HOSTED_ZONE_ID
            };
    
            const aws = require('aws-sdk');
            const route53 = new aws.Route53();
            route53.changeResourceRecordSets(deleteDnsParams, (err, data) => {
                console.log(err);
                console.log(data);
                if (err) {
                    reject(err);
                    return;
                }
                const deleteParams = {
                    Id: data.ChangeInfo.Id
                };

                route53.waitFor('resourceRecordSetsChanged', deleteParams, (waitErr, waitData) => {
                    if (waitErr) {
                        reject(waitErr);
                    } else if (waitData && waitData.ChangeInfo && waitData.ChangeInfo.Status === 'INSYNC') {
                        resolve();
                    } else {
                        reject(new Error('Route53 DELETE did not reach INSYNC: ' + JSON.stringify(waitData && waitData.ChangeInfo)));
                    }
                });

            });
        }).catch(err => {
            console.error('Error');
            console.error(err);
            reject(err);
        });
    
    });
    
    const getDnsRecord = (name) => new Promise((resolve, reject) => {
        const params = {
            HostedZoneId: AWS_ROUTE_53_HOSTED_ZONE_ID,
            StartRecordName: name,
            StartRecordType: 'TXT'
        };
    
        const aws = require('aws-sdk');
        const route53 = new aws.Route53();
        route53.listResourceRecordSets(params, (err, data) => {
            if (err) {
                console.error('error listing record sets');
                console.error(err);
                reject();
            } else {
                for (const i in data.ResourceRecordSets) {
                    const entry = data.ResourceRecordSets[i];
                    if (entry.Name === name + '.') {
                        resolve(entry.ResourceRecords[0].Value);
                    }
                }
                reject();
            }
        });
    
    });
    
    const insertCertRecord = (ip, cert) => new Promise((resolve, reject) => {
        getMongoCollection('certs').then((collection) => {
            collection.insertOne({
                ip,
                expiresAt: Date.now() + (60 * 24 * 60 * 60 * 1000), // 60 days from now
                cert
            }).then(() => {
                console.log('auyoao');
                // Must settle the promise: handleCertRequest chains .then(resolve)
                // on this, so without it a SUCCESSFUL issuance never acks and the
                // job gets redelivered.
                resolve();
            }).catch(reject);
        }).catch(reject);
    });

    // Look up an existing, not-yet-expired cert for this IP. Makes CERT_REQUEST
    // idempotent: a redelivered or double-submitted job reuses the stored cert
    // instead of issuing a fresh one (which would trip Let's Encrypt production's
    // rate limit).
    const getValidCert = (ip) => new Promise((resolve, reject) => {
        getMongoCollection('certs').then((collection) => {
            collection.findOne({ ip, expiresAt: { $gt: Date.now() } }).then(resolve).catch(reject);
        }).catch(reject);
    });

    const handleCertRequest = (data) => new Promise((resolve, reject) => {
        console.log('yoooo');
        console.log(data);

        getValidCert(data.ip).then((existing) => {
            if (existing) {
                // Already have a live cert for this IP — don't re-issue.
                console.log(`[job] cert already valid for ip=${data.ip} (expiresAt=${existing.expiresAt}) -> skipping issuance`);
                resolve();
                return;
            }

            const key = data.key.data;
            const client = new acme.Client({
                directoryUrl: acme.directory.letsencrypt.production,
                accountKey: key
            });

            console.log('did this !!');
            const csr = data.cert.data;
            console.log('this is csr ' + csr);
            const autoOpts = {
                csr,
                email: 'joseph@homegames.io',
                termsOfServiceAgreed: true,
                challengeCreateFn,//: async (authz, challenge, keyAuthorization) => {},
                challengeRemoveFn,//: async (authz, challenge, keyAuthorization) => {},
                challengePriority: ['dns-01']
            };

            // 5 min backstop: dns-01 propagation + LE validation can legitimately take
            // a couple minutes, but anything past this is a stuck flow, not slow DNS.
            withTimeout(client.auto(autoOpts), 5 * 60 * 1000, 'ACME client.auto').then(certificate => {
                console.log('certificate!');
                console.log(certificate);
                // Reject if the store fails, so a freshly-issued (rate-limited!) cert
                // that can't be persisted surfaces as a failure instead of hanging.
                insertCertRecord(data.ip, Buffer.from(certificate).toString('base64')).then(resolve).catch(reject);
            }).catch(err => {
                console.error('error creating certificate');
                console.error(err);
                // Propagate so the consumer can ack-and-drop instead of hanging silently.
                reject(err);
            });
        }).catch(reject);
    });
    

    // ---------------------------------------------------------------------------
    // LLM model server (persistent Python child)
    // ---------------------------------------------------------------------------
    // One long-lived child (llm/model_server.py) holds the warm MLX model. We
    // talk to it over newline-delimited JSON: write one job per line to stdin,
    // read one result per line from stdout. The child's stderr is its logging.

    let llmChild = null;            // current child process, or null if not running
    let llmReady = false;           // true once it emits {"ready":true}
    let llmStdoutBuf = '';          // partial-line buffer for child stdout
    const llmPending = new Map();   // job id -> { resolve, reject }
    const llmReadyWaiters = [];     // resolvers waiting for the child to become ready

    const rejectAllPending = (err) => {
        for (const { reject } of llmPending.values()) {
            reject(err);
        }
        llmPending.clear();
    };

    const handleLlmLine = (line) => {
        console.log('what the fuck!');
        let msg;
        try {
            msg = JSON.parse(line);
        } catch (e) {
            console.error('[llm] non-JSON line on stdout: ' + line);
            return;
        }
        if (msg.ready) {
            llmReady = true;
            console.log('[llm] model server ready');
            while (llmReadyWaiters.length) llmReadyWaiters.shift()();
            return;
        }
        const pending = msg.id != null ? llmPending.get(msg.id) : null;
        if (!pending) {
            console.error('[llm] result for unknown/expired id ' + msg.id);
            return;
        }
        llmPending.delete(msg.id);
        pending.resolve(msg);
    };

    const spawnModelServer = () => {
        console.log('[llm] spawning model server: ' + LLM_PYTHON + ' ' + LLM_SERVER_PATH);
        llmReady = false;
        llmStdoutBuf = '';
        const child = spawn(LLM_PYTHON, [LLM_SERVER_PATH], {
            stdio: ['pipe', 'pipe', 'pipe'],
            env: AUTHORING_DOC_PATH ? { ...process.env, AUTHORING_DOC_PATH } : process.env,
        });

        // stdout is the protocol channel: newline-delimited JSON only.
        child.stdout.setEncoding('utf8');
        child.stdout.on('data', (chunk) => {
            llmStdoutBuf += chunk;
            let nl;
            while ((nl = llmStdoutBuf.indexOf('\n')) >= 0) {
                const line = llmStdoutBuf.slice(0, nl).trim();
                llmStdoutBuf = llmStdoutBuf.slice(nl + 1);
                if (line) handleLlmLine(line);
            }
        });

        // stderr is the child's human-readable logging; surface it tagged.
        child.stderr.setEncoding('utf8');
        child.stderr.on('data', (chunk) => {
            process.stderr.write('[llm] ' + chunk);
        });

        const onGone = (info) => {
            if (llmChild !== child) return; // already replaced by a restart
            console.error('[llm] model server gone (' + info + ')');
            llmChild = null;
            llmReady = false;
            rejectAllPending(new Error('LLM model server exited (' + info + ')'));
        };
        child.on('exit', (code, signal) => onGone('code=' + code + ' signal=' + signal));
        child.on('error', (err) => onGone('spawn error: ' + err.message));

        llmChild = child;
    };

    // Ensure the child is running and ready; resolves once {"ready":true} seen.
    const ensureModelServer = () => new Promise((resolve, reject) => {
        if (!llmChild) {
            try {
                spawnModelServer();
            } catch (err) {
                reject(err);
                return;
            }
        }
        if (llmReady) {
            resolve();
        } else {
            llmReadyWaiters.push(resolve);
        }
    });

    // Kill the current child (e.g. after a stuck job) so the next job respawns it.
    const restartModelServer = () => {
        if (llmChild) {
            const dying = llmChild;
            llmChild = null;
            llmReady = false;
            try { dying.kill('SIGKILL'); } catch (e) { /* already dead */ }
        }
        rejectAllPending(new Error('LLM model server restarted'));
    };

    // Send one job to the model server, resolve with its result line.
    const runLlmJob = (job) => ensureModelServer().then(() => new Promise((resolve, reject) => {
        if (llmPending.has(job.id)) {
            reject(new Error('Duplicate in-flight LLM job id ' + job.id));
            return;
        }
        llmPending.set(job.id, { resolve, reject });
        llmChild.stdin.write(JSON.stringify(job) + '\n');
    }));

    // POST the model server's result to the API. result = {id, status, result?, error?}.
    // The job is only acked once this resolves, and with prefetch(1) an unacked
    // job blocks all further deliveries — so a fetch with NO timeout against a
    // slow/unreachable API would silently wedge the whole consumer. Bound each
    // attempt and retry a few times (the generated result is expensive to lose),
    // then give up so the consumer can ack-drop and move on.
    const postLlmResult = (requestId, result, attempt = 1) => {
        const MAX_ATTEMPTS = 3;
        const payload = { requestId, status: result.status };
        if (result.result != null) payload.result = result.result;
        if (result.error != null) payload.error = result.error;
        return fetch(`${API_URL}/internal/llm-result`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${LLM_WORKER_SECRET}`
            },
            body: JSON.stringify(payload),
            signal: AbortSignal.timeout(30000)
        }).then((resp) => {
            if (!resp.ok) {
                return resp.text().then((text) => {
                    throw new Error(`/internal/llm-result returned ${resp.status}: ${text.slice(0, 200)}`);
                });
            }
            console.log(`[llm] posted ${result.status} for ${requestId}`);
        }).catch((err) => {
            if (attempt < MAX_ATTEMPTS) {
                console.error(`[llm] post attempt ${attempt}/${MAX_ATTEMPTS} for ${requestId} failed (${err && err.message}); retrying`);
                return new Promise(r => setTimeout(r, 2000 * attempt))
                    .then(() => postLlmResult(requestId, result, attempt + 1));
            }
            throw err;
        });
    };

    const handleLlmRequest = (data) => new Promise((resolve, reject) => {
        // mode 'CREATE' = write a new game from the starter template (absent = edit)
        const job = { id: data.requestId, source: data.source, prompt: data.prompt, mode: data.mode };
        // 10 min backstop: covers a cold child's model load plus generation.
        withTimeout(runLlmJob(job), 10 * 60 * 1000, 'LLM model server').then(
            (result) => {
                // Generation succeeded (server still warm); relay to the API.
                postLlmResult(data.requestId, result).then(resolve).catch(reject);
            },
            (err) => {
                // Generation failed (timeout or child died) — the server may be
                // wedged, so restart it for the next job, then fail this one.
                console.error('[llm] generation failed for ' + data.requestId + ': ' + (err && err.message));
                restartModelServer();
                reject(err);
            }
        );
    });

    const messageHandlers = {
        [CERT_REQUEST]: {
            handle: handleCertRequest
        },
        [LLM_REQUEST]: {
            handle: handleLlmRequest
        }
    };
    
    const handleMessage = (message) => new Promise((resolve, reject) => {
        console.log('hi jsdnsd');
        console.log(message);
        let data = null;
        try {
            data = JSON.parse(message.content);
        } catch (err) {
            reject(err);
        }
    
        if (data) {
            if (!data.type) {
                reject('Missing type');
            } else {
                if (messageHandlers[data.type]) {
                    messageHandlers[data.type].handle(data).then(resolve).catch(reject);
                } else {
                    reject('Invalid type ' + data.type);
                }
            }
    
        } 
    });

    const innerRun = (connection) => new Promise((resolve, reject) => {
        connection.createChannel((channelError, channel) => {
            if (channelError) {
                reject(channelError);
            } else {
                channel.assertQueue(QUEUE_NAME, {
                    durable: true
                });
                // Warn if another consumer is already attached: RabbitMQ round-robins
                // deliveries across ALL consumers, so a stray second worker (or a
                // leftover process) silently steals a share of the jobs — they just
                // never show up here. Run exactly one worker. (consumerCount here
                // excludes us, since we haven't called consume() yet.)
                channel.checkQueue(QUEUE_NAME, (qErr, ok) => {
                    if (!qErr && ok && ok.consumerCount > 0) {
                        console.warn(`[warn] ${ok.consumerCount} other consumer(s) already attached to ` +
                            `${QUEUE_NAME} — jobs will be round-robined and some will NOT reach this worker. ` +
                            `Kill the strays (pkill -f "node index.js") and run a single instance.`);
                    }
                });
                // One unacked message at a time. Without this the broker pushes the
                // entire backlog to a single consumer at once; cert jobs run for
                // minutes, so a connection flap would strand (and later redeliver)
                // every in-flight job instead of just one.
//                channel.prefetch(1);
                console.log('listening to messages on ' + QUEUE_NAME + ' at ' + REQUEST_QUEUE_URL);
                channel.consume(QUEUE_NAME, (msg) => {
                    console.log('got a damn message');
                    console.log(msg);
                    // null msg means the consumer was cancelled by the broker.
                    if (!msg) return;

                    // Greppable per-delivery summary (grep '[job]') so duplicate
                    // processing is easy to spot. The distinction that matters:
                    //   redelivered=false  -> the broker handed us this delivery for
                    //                         the first time. Two such lines with the
                    //                         SAME ip but DIFFERENT deliveryTags means
                    //                         the job was ENQUEUED twice (producer side).
                    //   redelivered=true   -> the broker is RE-pushing a message we
                    //                         never acked (connection dropped mid-job,
                    //                         or we acked on a dead channel). Same job,
                    //                         reprocessed. This is a consumer-side bug,
                    //                         not a double submit.
                    // consumerTag tells you WHICH consumer got it — if you see more
                    // than one distinct consumerTag live at once, the reconnect logic
                    // has spawned overlapping consumers (see queue-stats).
                    let summary = {};
                    try {
                        const parsed = JSON.parse(msg.content);
                        summary = { type: parsed.type, ip: parsed.ip };
                    } catch (e) { /* parse error surfaced by handleMessage below */ }
                    console.log(`[job] recv type=${summary.type} ip=${summary.ip} redelivered=${msg.fields.redelivered} deliveryTag=${msg.fields.deliveryTag} consumerTag=${msg.fields.consumerTag} messageId=${msg.properties.messageId}`);

                    handleMessage(msg).then(() => {
                        console.log(`[job] done type=${summary.type} ip=${summary.ip} deliveryTag=${msg.fields.deliveryTag} -> ack`);
                        // Ack on success so it's removed from the queue. NOTE: if the
                        // channel that delivered this msg has since closed (reconnect),
                        // this ack throws / no-ops and the broker will redeliver.
                        channel.ack(msg);
                    }).catch(err => {
                        console.error(`[job] fail type=${summary.type} ip=${summary.ip} deliveryTag=${msg.fields.deliveryTag} -> ack(drop)`);
                        console.error(err);
                        // Application-level failure: ack to DROP (do NOT requeue).
                        // Auto-retrying a failed ACME order would hammer Let's Encrypt's
                        // rate limit; the user can re-request to enqueue a fresh job.
                        // A worker CRASH (no ack at all) still triggers redelivery.
                        channel.ack(msg);
                    });
                }, {
                    // Manual ack: a crash before ack redelivers the job (crash-safety),
                    // which noAck:true did not provide.
                    noAck: false
                });
                resolve();
            }
        });
    });
    
    // Purge every message currently sitting in the queue. Opens its own
    // short-lived connection/channel so it can be called independently of the
    // long-running consumer (e.g. from the CLI). Resolves with the number of
    // messages that were removed.
    const clearQueue = () => new Promise((resolve, reject) => {
        amqp.connect(REQUEST_QUEUE_URL, { 'heartbeat': HEARTBEAT_SECONDS }, (connectionError, connection) => {
            if (connectionError) {
                reject(connectionError);
                return;
            }
            connection.createChannel((channelError, channel) => {
                if (channelError) {
                    connection.close();
                    reject(channelError);
                    return;
                }
                // assertQueue first so purge doesn't fail if the queue is absent,
                // and to match the durable settings the consumer declares.
                channel.assertQueue(QUEUE_NAME, { durable: true });
                channel.purgeQueue(QUEUE_NAME, (purgeError, ok) => {
                    connection.close();
                    if (purgeError) {
                        reject(purgeError);
                    } else {
                        console.log(`Purged ${ok.messageCount} message(s) from ${QUEUE_NAME}`);
                        resolve(ok.messageCount);
                    }
                });
            });
        });
    });

    // Passive queue inspection: returns live message depth and the number of
    // consumers currently attached to the queue, WITHOUT modifying anything.
    // consumerCount > 1 is the tell-tale sign that the reconnect logic in run()
    // has spawned overlapping consumers — each connection flap can leave the old
    // consumer attached while a new one starts, and a redelivered job can then be
    // processed by whichever consumer grabs it. Run `node index.js queue-stats`.
    const queueStats = () => new Promise((resolve, reject) => {
        amqp.connect(REQUEST_QUEUE_URL, { 'heartbeat': HEARTBEAT_SECONDS }, (connectionError, connection) => {
            if (connectionError) {
                reject(connectionError);
                return;
            }
            connection.createChannel((channelError, channel) => {
                if (channelError) {
                    connection.close();
                    reject(channelError);
                    return;
                }
                channel.checkQueue(QUEUE_NAME, (err, ok) => {
                    connection.close();
                    if (err) {
                        reject(err);
                    } else {
                        console.log(`${QUEUE_NAME}: messages=${ok.messageCount} consumers=${ok.consumerCount}`);
                        resolve(ok);
                    }
                });
            });
        });
    });

    const run = () => new Promise((resolve, reject) => {
        console.log("RIRIRI" + REQUEST_QUEUE_URL);
        amqp.connect(REQUEST_QUEUE_URL, { 'heartbeat': HEARTBEAT_SECONDS }, (connectionError, connection) => {
            if (connectionError) {
                // Let the setInterval guard retry; don't reconnect inline.
                running = false;
                reject(connectionError);
                return;
            }

            // error and close both fire on a dropped connection (error precedes
            // close), so reconnecting inline from each handler double-connects and
            // stacks overlapping consumers. Instead just release the guard: the
            // setInterval below restarts exactly ONE connection on its next tick.
            connection.on('error', (err) => {
                console.error("queue error");
                console.error(err);
                running = false;
            });

            connection.on('close', () => {
                console.warn('queue connection closed');
                running = false;
            });

            innerRun(connection).then(resolve).catch(reject);
        });
    });
    
    // Run `node index.js clear-queue` to empty the queue and exit, without
    // starting the consumer loop.
    if (process.argv.includes('clear-queue') || process.argv.includes('--clear-queue')) {
        clearQueue().then((count) => {
            console.log(`Done. Removed ${count} job(s) from ${QUEUE_NAME}.`);
            process.exit(0);
        }).catch((err) => {
            console.error('Failed to clear queue');
            console.error(err);
            process.exit(1);
        });
        return;
    }

    // Run `node index.js queue-stats` to print queue depth + consumer count and exit.
    if (process.argv.includes('queue-stats') || process.argv.includes('--queue-stats')) {
        queueStats().then(() => {
            process.exit(0);
        }).catch((err) => {
            console.error('Failed to read queue stats');
            console.error(err);
            process.exit(1);
        });
        return;
    }

    // Don't leave the Python model server orphaned when the worker stops.
    // SIGKILL (not SIGTERM): during model load the child is in a native MLX
    // call where Python defers signal handlers, so SIGTERM can be ignored long
    // enough to orphan it. The server has no state to flush, so a hard kill is
    // fine. (If node itself crashes without running this, the child still exits
    // on its own once it finishes loading and reads EOF on the closed stdin.)
    const shutdown = (signal) => {
        console.log('received ' + signal + ', shutting down');
        if (llmChild) {
            try { llmChild.kill('SIGKILL'); } catch (e) { /* already dead */ }
        }
        process.exit(0);
    };
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

    // Eagerly spawn + warm the model server at boot (not lazily on the first
    // job), so the model is loaded and the generation path is compiled before
    // any LLM_REQUEST arrives. Placed after the CLI early-returns so queue-stats
    // / clear-queue don't trigger a model load. Best effort: if it fails (e.g.
    // missing venv on a cert-only box), the first LLM job will retry the spawn.
    ensureModelServer()
        .then(() => console.log('[llm] model server warmed and ready at startup'))
        .catch((err) => console.error('[llm] startup warm failed (will retry on first job): ' + (err && err.message)));

    // forever
    setInterval(() => {
        if (!running) {
            running = true;
            run().then(() => {
            }).catch((err) => {
                console.log(err);
                running = false;
            });
        }
    },500);

})();
