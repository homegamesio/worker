const amqp = require('amqplib/callback_api');
const crypto = require("crypto");
const acme = require('acme-client');
const { v4: uuidv4 } = require("uuid");
const { MongoClient } = require('mongodb');


(async () => {
    //const {LlamaModel, LlamaContext, LlamaChatSession} = await import("node-llama-cpp");

    //console.log(LlamaModel);
    //const model = new LlamaModel({
    //    modelPath: '/Users/josephgarcia/Downloads/mistral-7b-instruct-v0.2.Q4_K_M.gguf'
    //});
    
    
    const fs = require('fs');
    const { exec } = require('child_process');
    const http = require('http');
    const https = require('https');

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
    
    const PUBLISH_REQUEST = 'PUBLISH_REQUEST';
    const CONTENT_REQUEST = 'CONTENT_REQUEST';
    const PROFILE_IMAGE_APPROVAL_REQUEST = 'PROFILE_IMAGE_APPROVAL_REQUEST';
    const GAME_IMAGE_APPROVAL_REQUEST = 'GAME_IMAGE_APPROVAL_REQUEST';
    const CERT_REQUEST = 'CERT_REQUEST';
    const BUILD_GAME = 'BUILD_GAME';
    
    const PUBLISH_REQUEST_TABLE = 'publishRequests';
    const GAME_VERSION_TABLE = 'gameVersions';
    
    const FORGEJO_URL = process.env.FORGEJO_URL || 'http://52.32.110.71:3000';
    
    let running = false;
    
    const API_URL = process.env.API_URL;
    
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
    
    const getPublishRequest = (requestId) => new Promise((resolve, reject) => {
        getMongoCollection(PUBLISH_REQUEST_TABLE).then((collection) => {
            collection.findOne({ requestId }).then(publishRequest => {
                resolve({
                    userId: publishRequest.userId,
                    assetId: publishRequest.assetId,
                    gameId: publishRequest.gameId,
                    versionId: publishRequest.versionId,
                    requestId: publishRequest.requestId,
                    'status': publishRequest['status']
                });
            });
        }).catch(reject);
    });
    
    const getHash = (input) => {
      return crypto.createHash("md5").update(input).digest("hex");
    };
    
    const generateId = () => getHash(uuidv4());
    
    const publishVersion = (squishVersion, versionId, data) => new Promise((resolve, reject) => {
        getMongoCollection(GAME_VERSION_TABLE).then(collection => {
            const gameVersion = { 
                gameId: data.gameId, 
                versionId, 
                requestId: data.requestId, 
                publishedAt: Date.now(), 
                publishedBy: data.userId, 
                sourceAssetId: data.assetId,
                squishVersion: squishVersion || ''
            };
            console.log("PUBLISHING THIS OPNE");
            console.log(data);
            
            collection.insertOne(gameVersion).then(() => {
                getMongoCollection('publishRequests').then(coll => {
                    console.log("FOGOGOGOG");
                    console.log(data.requestId);
                    coll.updateOne({ requestId: data.requestId }, { "$set": { 'status': 'CONFIRMED'} }).catch(reject).then(resolve);
    
                });
            }).catch((err) => {
                console.error('Failed to publish new version');
                console.error(err);
                reject(err);
            });
        });
    });
    
    const getMongoDocument = (assetId) => new Promise((resolve, reject) => {
        getMongoCollection('documents').then(documents => {
            documents.findOne({ assetId }).then(doc => {
                if (doc) {
                    resolve(doc);
                } else {
                    reject('not found');
                }
            });
        });
    });
    
    const poke = (requestRecord, filePath) => new Promise((resolve, reject) => {
        console.log('need to run dockerr thing');
        console.log(requestRecord);
        const publishEvent = {};
        const { exec } = require("child_process");
        const cmd = `docker run -v ${filePath}:/thangs/test.zip --rm tang2`;
        console.log(cmd);
        const ting = exec(cmd, (err, stderr, stdout) => {
            console.log('eeoeoeoe');
            console.log(err);
            console.log(stderr);
            console.log(stdout);
            const lines = stderr && stderr.split("\\n");
            let exitMessage = null;
            if (lines) {
              for (line in lines) {
                const ting = stderr.match(
                  "AYYYYYYYYYLMAOTHISISTHEEXITMESSAGE:(.+)::andthatwastheendofthemessage",
                );
                if (ting) {
                  console.log("TING!!!!");
                  console.log(ting);
                  if (ting[1]) {
                    if (exitMessage) {
                      console.error("Multiple exit messages found");
                      throw new Error("nope nope nope multiple exit messages");
                    }
                    exitMessage = ting[1];
                    if (exitMessage.startsWith("success")) {
                        const squishVersion = exitMessage.split('-')[1];
                      resolve(squishVersion);
                    } else {
                      reject("Failed: " + exitMessage);
                    }
                  }
                }
              }
            } else {
                reject('no output');
            }
        });
    });
    
    const handlePublishRequest = (data) => new Promise((resolve, reject) => {
        const { requestId, gameId, userId, assetId } = data;
        if (!requestId || !gameId || !userId || !assetId) {
            reject('Invalid payload: ' + data);
        } else {
            getPublishRequest(requestId).then(requestRecord => {
                console.log("this is request");
                console.log(requestRecord);
                getMongoDocument(requestRecord.assetId).then((doc) => {
                    console.log('got doc nee to download');
                    console.log(doc);
                    const filePath = '/Users/josephgarcia/homedome_data/' + Date.now() + Math.floor(Math.random()) + '.zip';
                    console.log('dodododododo ' + filePath);
                    fs.writeFileSync(filePath, doc.data.buffer);
                    console.log('wrote to ' + filePath);
                    poke(requestRecord, filePath).then((squishVersion) => {
                        publishVersion(squishVersion, requestRecord.versionId, data).then(resolve).catch(reject);
                    });
                });
            }).catch(reject);
        }
    });
    
    const handleContentRequest = (_data) => new Promise((resolve, reject) => {
        console.log('nice cool');
        console.log(_data);
        const request = _data.data;
        const req = JSON.parse(request);
        const p = req.prompt;
        const context = new LlamaContext({model});
        const session = new LlamaChatSession({context});
    
        session.prompt(p).then((data) => {
            console.log('got data');
            console.log(data);
            getMongoCollection('contentRequests').then((collection) => {
                collection.findOne({ requestId: req.requestId }).then(found => {
                    console.log('ayo');
                    console.log(found);
                    if (!found) {
                        reject('Original request not found');
                    } else {
                        collection.updateOne({ requestId: req.requestId }, { "$set": { response: data } }).then(() => {
                            console.log('heyooooo');
                            console.log(data);
                            resolve(data);
                        });
                    }
                });
            });
        });
    
    });
    
    const setImage = (userId, assetId) => new Promise((resolve, reject) => {
        getMongoCollection('users').then(users => {
            users.findOne({ userId }).then((foundUser) => {
                if (!foundUser) {
                    reject('User not found');
                } else {
                    users.updateOne({ userId }, { "$set": { image: assetId } }).catch(reject).then(resolve);
                }
            });
        });
        console.log('gonna set image');
    });
    
    const setGameImage = (userId, gameId, assetId) => new Promise((resolve, reject) => {
        getMongoCollection('users').then(users => {
            users.findOne({ userId }).then((foundUser) => {
                if (!foundUser) {
                    reject('User not found');
                } else {
                    getMongoCollection('games').then(games => {
                        games.findOne({ gameId }).then((foundGame) => {
                            if (!foundGame) {
                                reject('Game not found');
                            } else {
                                games.updateOne({ gameId }, {"$set": { thumbnail: assetId }}).catch(reject).then(resolve);
                            }
                        });
                    });
                }
            });
        });
    });
    
    const handleProfileImageApprovalRequest = (data) => new Promise((resolve, reject) => {
        const { userId, assetId} = data;
        downloadAsset(assetId).then(assetPath => {
            exec(`bash run.sh ${assetPath}`, (err, stdout, stderr) => {
                if (stdout.trim() === 'fail') {
                    console.warn(`nsfw image - ${assetId}`);
                    reject('NSFW');
                } else if (stdout.trim() === 'success') {
                    console.log(`setting profile image to ${assetId} for ${userId}`);
                    setImage(userId, assetId);
                }
            });
        });
    });
    
    const handleGameImageApprovalRequest = (data) => new Promise((resolve, reject) => {
        const { userId, gameId, assetId} = data;
        downloadAsset(assetId).then(assetPath => {
            exec(`bash run.sh ${assetPath}`, (err, stdout, stderr) => {
                if (stdout.trim() === 'fail') {
                    console.warn(`nsfw image - ${assetId}`);
                    reject('NSFW');
                } else if (stdout.trim() === 'success') {
                    console.log(`setting game ${gameId} image to ${assetId} for ${userId}`);
                    setGameImage(userId, gameId, assetId);
                }
            });
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
                directoryUrl: acme.directory.letsencrypt.staging,
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
    
    const downloadAsset = (assetId) => new Promise((resolve, reject) => {
        const outPath = '/Users/josephgarcia/nsfw_model/assets/' + assetId;
        const writeStream = fs.createWriteStream(outPath);
    
        writeStream.on('close', () => {
            resolve(outPath);
        });
    
        https.get(`${API_URL}/assets/${assetId}`, (res) => {//assets.homegames.io/${assetId}`, (res) => {
            console.log('downloaded');
            res.pipe(writeStream);
        });
    });
    
    // ---------------------------------------------------------------------------
    // BUILD_GAME handler (Forgejo-based pipeline)
    // ---------------------------------------------------------------------------

    const updateBuildStatus = (buildId, status, error) => new Promise((resolve, reject) => {
        getMongoCollection('builds').then(collection => {
            const update = { '$set': { status, completed: Date.now() } };
            if (error) {
                update['$set'].error = error;
            }
            collection.updateOne({ buildId }, update).then(resolve).catch(reject);
        }).catch(reject);
    });

    const publishGameVersion = (data) => new Promise((resolve, reject) => {
        const versionId = generateId();
        const gameVersion = {
            gameId: data.gameId,
            versionId,
            commitSha: data.commitSha,
            publishedAt: Date.now(),
            publishedBy: data.userId,
            forgejoRepo: data.forgejoRepo,
        };

        getMongoCollection(GAME_VERSION_TABLE).then(collection => {
            collection.insertOne(gameVersion).then(() => {
                console.log(`Published game version ${versionId} for ${data.gameId}`);
                resolve(versionId);
            }).catch(reject);
        }).catch(reject);
    });

    const updateElasticsearch = (gameId) => new Promise((resolve, reject) => {
        getMongoCollection('games').then(collection => {
            collection.findOne({ gameId }).then(game => {
                if (!game) {
                    resolve();
                    return;
                }

                const body = JSON.stringify({
                    gameId: game.gameId,
                    name: game.name,
                    description: game.description || '',
                    developerId: game.developerId,
                    created: game.created,
                    thumbnail: game.thumbnail,
                    featured: game.featured || false,
                });

                const { ELASTICSEARCH_HOST, ELASTICSEARCH_PORT } = process.env;
                if (!ELASTICSEARCH_HOST) {
                    console.log('No ELASTICSEARCH_HOST configured, skipping index update');
                    resolve();
                    return;
                }

                const options = {
                    hostname: ELASTICSEARCH_HOST,
                    port: ELASTICSEARCH_PORT || 9200,
                    path: `/games/_doc/${gameId}`,
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Content-Length': Buffer.byteLength(body),
                    },
                };

                const req = http.request(options, (res) => {
                    let data = '';
                    res.on('data', (chunk) => { data += chunk; });
                    res.on('end', () => { resolve(); });
                });
                req.on('error', (e) => {
                    console.error('Elasticsearch update failed', e.message);
                    resolve(); // Don't fail the build over search indexing
                });
                req.write(body);
                req.end();
            }).catch(reject);
        }).catch(reject);
    });

    const handleBuildGame = (data) => new Promise((resolve, reject) => {
        const { buildId, gameId, forgejoRepo, commitSha, userId } = data;

        if (!buildId || !gameId || !forgejoRepo || !commitSha) {
            const errMsg = 'Invalid BUILD_GAME payload: missing required fields';
            console.error(errMsg, data);
            reject(errMsg);
            return;
        }

        console.log(`[BUILD_GAME] Starting build ${buildId} for ${forgejoRepo}@${commitSha.substring(0, 7)}`);

        // TODO: Replace this with real Docker sandbox check.
        // For now, we fake a successful build after a short delay to simulate
        // the Docker container running and passing.
        //
        // When real: clone repo from Forgejo, zip it, run through tang2 Docker
        // container, parse exit message for success/failure.
        //
        // Real implementation would look like:
        //   1. git clone ${FORGEJO_URL}/${forgejoRepo}.git --branch main --single-branch /tmp/build-${buildId}
        //   2. cd /tmp/build-${buildId} && git checkout ${commitSha}
        //   3. zip -r /tmp/build-${buildId}.zip /tmp/build-${buildId}/
        //   4. docker run -v /tmp/build-${buildId}.zip:/thangs/test.zip --rm tang2
        //   5. Parse stderr for AYYYYYYYYYLMAOTHISISTHEEXITMESSAGE

        const FAKE_BUILD_DELAY_MS = 2000;

        setTimeout(() => {
            const buildPassed = true; // Fake: always passes

            if (buildPassed) {
                console.log(`[BUILD_GAME] Build ${buildId} passed sandbox check`);

                publishGameVersion(data).then(versionId => {
                    updateBuildStatus(buildId, 'PUBLISHED', null).then(() => {
                        console.log(`[BUILD_GAME] Build ${buildId} published as version ${versionId}`);

                        updateElasticsearch(gameId).then(() => {
                            resolve();
                        }).catch(err => {
                            console.error('[BUILD_GAME] Elasticsearch update failed (non-fatal)', err);
                            resolve(); // Build still succeeded
                        });
                    }).catch(err => {
                        console.error(`[BUILD_GAME] Failed to update build status for ${buildId}`, err);
                        reject(err);
                    });
                }).catch(err => {
                    console.error(`[BUILD_GAME] Failed to publish game version for ${buildId}`, err);
                    updateBuildStatus(buildId, 'FAILED', 'Failed to create game version: ' + (err.toString ? err.toString() : err))
                        .then(() => reject(err))
                        .catch(() => reject(err));
                });
            } else {
                // This path would be hit when the Docker sandbox rejects the code
                const errorMessage = 'Sandbox validation failed';
                console.log(`[BUILD_GAME] Build ${buildId} failed: ${errorMessage}`);

                updateBuildStatus(buildId, 'FAILED', errorMessage).then(() => {
                    resolve(); // Message handled successfully even though build failed
                }).catch(err => {
                    console.error(`[BUILD_GAME] Failed to update build status for ${buildId}`, err);
                    reject(err);
                });
            }
        }, FAKE_BUILD_DELAY_MS);
    });

    const messageHandlers = {
        [PUBLISH_REQUEST]: {
            handle: handlePublishRequest
        },
        [CONTENT_REQUEST]: {
            handle: handleContentRequest
        },
        [PROFILE_IMAGE_APPROVAL_REQUEST]: {
            handle: handleProfileImageApprovalRequest
        },
        [GAME_IMAGE_APPROVAL_REQUEST]: {
            handle: handleGameImageApprovalRequest
        },
        [CERT_REQUEST]: {
            handle: handleCertRequest
        },
        [BUILD_GAME]: {
            handle: handleBuildGame
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
                // One unacked message at a time. Without this the broker pushes the
                // entire backlog to a single consumer at once; cert jobs run for
                // minutes, so a connection flap would strand (and later redeliver)
                // every in-flight job instead of just one.
                channel.prefetch(1);
                console.log('listening to messages on ' + QUEUE_NAME + ' at ' + REQUEST_QUEUE_URL);
                channel.consume(QUEUE_NAME, (msg) => {
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
