const amqp = require('amqplib/callback_api');
const { MongoClient } = require('mongodb');

const fs = require('fs');
const { exec } = require('child_process');
const http = require('http');

const REQUEST_QUEUE_URL = 'amqp://localhost';//process.env.QUEUE_URL;
const QUEUE_NAME = 'homegames-jobs';//process.env.QUEUE_NAME;

const PUBLISH_REQUEST = 'PUBLISH_REQUEST';
const CONTENT_REQUEST = 'CONTENT_REQUEST';
const PROFILE_IMAGE_APPROVAL_REQUEST = 'PROFILE_IMAGE_APPROVAL_REQUEST';

const PUBLISH_REQUEST_TABLE = 'publishRequests';
const GAME_VERSION_TABLE = 'gameVersions';

let running = false;

const DB_NAME = 'homegames';//process.env.DB_NAME;
const DB_HOST = 'localhost';//'52.32.110.71';//process.env.DB_HOST;
const DB_PORT = 27017;//process.env.DB_PORT
const DB_USERNAME = '';//'twerker';//process.env.DB_USERNAME
const DB_PASSWORD = '';//'xaxgur-7xusjo-Gojwej';//process.env.DB_USERNAME

const getMongoClient = () => {
    const uri = DB_USERNAME ? `mongodb://${DB_USERNAME}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/homegames` : `mongodb://${DB_HOST}:${DB_PORT}/homegames`;
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
                requestId: publishRequest.requestId,
                'status': publishRequest['status']
            });
        });
    }).catch(reject);
});

const poke = (publishEvent, requestRecord) => new Promise((resolve, reject) => {
    console.log("need to poke. im already docker");
});

const publishVersion = (publishEvent, requestRecord) => new Promise((resolve, reject) => {
    getMongoCollection(GAME_VERSION_TABLE).then(collection => {
        const gameVersion = { 
            gameId: requestRecord.gameId, 
            versionId: generateId(), 
            requestId: requestRecord.requestId, 
            publishedAt: Date.now(), 
            publishedBy: requestRecord.userId, 
            sourceAssetId: requestRecord.assetId 
        };
        
        collection.insertOne(gameVersion).then(() => {
            resolve(gameVersion);
        }).catch((err) => {
            console.error('Failed to publish new version');
            console.error(err);
            reject(err);
        });
    });
});

const handlePublishRequest = (data) => new Promise((resolve, reject) => {
    const { requestId, gameId, userId, assetId } = data;
    if (!requestId || !gameId || !userId || !assetId) {
        reject('Invalid payload: ' + data);
    } else {
        getPublishRequest(requestId).then(requestRecord => {
            poke(publishEvent, requestRecord).then(() => {
                publishVersion(publishEvent, requestRecord).then(resolve).catch(reject);
            });
        }).catch(reject);
    }
});

const handleContentRequest = (data) => new Promise((resolve, reject) => {

});

const setImage = (userId, assetId) => new Promise((resolve, reject) => {
    getMongoCollection('users').then(users => {
        users.findOne({ userId }).then((foundUser) => {
            console.log('fiouffofu');
            console.log(foundUser);
            if (!foundUser) {
                reject('User not found');
            } else {
                users.updateOne({ userId }, { "$set": { image: assetId } }).catch(reject).then(resolve);
            }
        });
    });
    console.log('gonna set image');
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

const downloadAsset = (assetId) => new Promise((resolve, reject) => {
    const outPath = '/Users/josephgarcia/nsfw_model/assets/' + assetId;
    const writeStream = fs.createWriteStream(outPath);

    writeStream.on('close', () => {
        resolve(outPath);
    });

    http.get(`http://localhost:82/assets/${assetId}`, (res) => {//assets.homegames.io/${assetId}`, (res) => {
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
    }
};

const handleMessage = (message) => new Promise((resolve, reject) => {
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
    amqp.connect(REQUEST_QUEUE_URL, (connectionError, connection) => {
        if (connectionError) {
            reject(connectionError);
        } else {
            connection.createChannel((channelError, channel) => {
                if (channelError) {
                    reject(channelError);
                } else {
                    channel.assertQueue(QUEUE_NAME, {
                        durable: false
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
//                        noAck: true
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
