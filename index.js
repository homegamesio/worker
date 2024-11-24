const amqp = require('amqplib/callback_api');
const crypto = require("crypto");
const acme = require('acme-client');
const { v4: uuidv4 } = require("uuid");
const { MongoClient } = require('mongodb');


(async () => {
    const {LlamaModel, LlamaContext, LlamaChatSession} = await import("node-llama-cpp");

    console.log(LlamaModel);
    const model = new LlamaModel({
        modelPath: '/Users/josephgarcia/Downloads/mistral-7b-instruct-v0.2.Q4_K_M.gguf'
    });
    
    
    const fs = require('fs');
    const { exec } = require('child_process');
    const http = require('http');
    const https = require('https');
    
    const REQUEST_QUEUE_URL = process.env.QUEUE_URL;
    const QUEUE_NAME = process.env.QUEUE_NAME;
    
    const PUBLISH_REQUEST = 'PUBLISH_REQUEST';
    const CONTENT_REQUEST = 'CONTENT_REQUEST';
    const PROFILE_IMAGE_APPROVAL_REQUEST = 'PROFILE_IMAGE_APPROVAL_REQUEST';
    const GAME_IMAGE_APPROVAL_REQUEST = 'GAME_IMAGE_APPROVAL_REQUEST';
    const CERT_REQUEST = 'CERT_REQUEST';
    
    const PUBLISH_REQUEST_TABLE = 'publishRequests';
    const GAME_VERSION_TABLE = 'gameVersions';
    
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
    
    const publishVersion = (versionId, data) => new Promise((resolve, reject) => {
        getMongoCollection(GAME_VERSION_TABLE).then(collection => {
            const gameVersion = { 
                gameId: data.gameId, 
                versionId, 
                requestId: data.requestId, 
                publishedAt: Date.now(), 
                publishedBy: data.userId, 
                sourceAssetId: data.assetId 
            };
            
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
                    if (exitMessage === "success") {
                      resolve();
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
                    poke(requestRecord, filePath).then(() => {
                        publishVersion(requestRecord.versionId, data).then(resolve).catch(reject);
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
    
                route53.waitFor('resourceRecordSetsChanged', params, (err, data) => {
                    if (data.ChangeInfo.Status === 'INSYNC') {
                        resolve();
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
                const deleteParams = {
                    Id: data.ChangeInfo.Id
                };
    
                route53.waitFor('resourceRecordSetsChanged', deleteParams, (err, data) => {
                    if (data.ChangeInfo.Status === 'INSYNC') {
                        resolve();
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
            });
        });
    });
    
    const handleCertRequest = (data) => new Promise((resolve, reject) => {
        console.log('yoooo');
        console.log(data);
        const key = data.key.data;
        const client = new acme.Client({
            directoryUrl: acme.directory.letsencrypt.production,//.staging
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
    
        client.auto(autoOpts).then(certificate => {
            console.log('certificate!');
            console.log(certificate);
            insertCertRecord(data.ip, Buffer.from(certificate).toString('base64')).then(resolve);
        }).catch(err => {
            console.error('error creating certificate');
            console.error(err);
        });
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
    
    const run = () => new Promise((resolve, reject) => {
        console.log('gonna connect');
        amqp.connect(REQUEST_QUEUE_URL, (connectionError, connection) => {
            console.log('connected');
            if (connectionError) {
                reject(connectionError);
            } else {
                connection.createChannel((channelError, channel) => {
                    if (channelError) {
                        reject(channelError);
                    } else {
                        channel.assertQueue(QUEUE_NAME, {
                            durable: true
                        });
                        console.log('listening to messages on ' + QUEUE_NAME + ' at ' + REQUEST_QUEUE_URL);
                        channel.consume(QUEUE_NAME, (msg) => {
                            console.log("Got message");
                            console.log(msg);
                            handleMessage(msg).then(() => {
                                console.log("Handled message successfully");
                            }).catch(err => {
                                console.error(err);
                            });
                        }, {
                            noAck: true
                        });
                        resolve();
                    }
                });
            }
        });
    });
    
    // forever
    setInterval(() => {
        if (!running) {
            run().then(() => {
                running = true;
            }).catch((err) => {
                console.log(err);
                running = false;
            });
        }
    },500);

})();
