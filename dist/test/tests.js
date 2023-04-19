"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
Object.defineProperty(exports, "__esModule", { value: true });
var chai = require("chai");
var chaiAsPromised = require("chai-as-promised");
var simple_mock_1 = require("simple-mock");
var iterall_1 = require("iterall");
var mqtt = require("mqtt");
var mqtt_pubsub_1 = require("../mqtt-pubsub");
chai.use(chaiAsPromised);
var expect = chai.expect;
var listener;
var publishSpy = simple_mock_1.spy(function (channel, message) { return listener && listener(channel, message); });
var subscribeSpy = simple_mock_1.spy(function (topic, options, cb) { return cb && cb(null, [__assign(__assign({}, options), { topic: topic })]); });
var unsubscribeSpy = simple_mock_1.spy(function (channel, _, cb) { return cb && cb(channel); });
var mqttPackage = mqtt;
var connect = function () {
    return {
        publish: publishSpy,
        subscribe: subscribeSpy,
        unsubscribe: unsubscribeSpy,
        on: function (event, cb) {
            if (event === 'message') {
                listener = cb;
            }
        },
    };
};
mqttPackage['connect'] = connect;
describe('MQTTPubSub', function () {
    var pubSub = new mqtt_pubsub_1.MQTTPubSub();
    it('can subscribe to specific mqtt channel and called when a message is published on it', function (done) {
        var sub;
        var onMessage = function (message) {
            pubSub.unsubscribe(sub);
            try {
                expect(message).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        pubSub.subscribe('Posts', onMessage).then(function (subId) {
            expect(subId).to.be.a('number');
            pubSub.publish('Posts', 'test');
            sub = subId;
        }).catch(function (err) { return done(err); });
    });
    it('can unsubscribe from specific mqtt channel', function (done) {
        pubSub.subscribe('Posts', function () { return null; }).then(function (subId) {
            pubSub.unsubscribe(subId);
            try {
                expect(unsubscribeSpy.callCount).to.equals(1);
                var call = unsubscribeSpy.lastCall;
                expect(call.args).to.have.members(['Posts']);
                done();
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('cleans up correctly the memory when unsubscribing', function (done) {
        Promise.all([
            pubSub.subscribe('Posts', function () { return null; }),
            pubSub.subscribe('Posts', function () { return null; }),
        ])
            .then(function (_a) {
            var subId = _a[0], secondSubId = _a[1];
            try {
                expect(pubSub.subscriptionMap[subId]).not.to.be.an('undefined');
                pubSub.unsubscribe(subId);
                expect(pubSub.subscriptionMap[subId]).to.be.an('undefined');
                expect(function () { return pubSub.unsubscribe(subId); }).to.throw("There is no subscription of id \"" + subId + "\"");
                pubSub.unsubscribe(secondSubId);
                done();
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('will not unsubscribe from the mqtt channel if there is another subscriber on it\'s subscriber list', function (done) {
        var lastSubId;
        var onMessage = function (msg) {
            pubSub.unsubscribe(lastSubId);
            expect(unsubscribeSpy.callCount).to.equals(1);
            try {
                expect(msg).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        var subscriptionPromises = [
            pubSub.subscribe('Posts', function () {
                done('Not supposed to be triggered');
            }),
            pubSub.subscribe('Posts', onMessage),
        ];
        Promise.all(subscriptionPromises).then(function (subIds) {
            try {
                expect(subIds.length).to.equals(2);
                pubSub.unsubscribe(subIds[0]);
                expect(unsubscribeSpy.callCount).to.equals(0);
                pubSub.publish('Posts', 'test');
                lastSubId = subIds[1];
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('will subscribe to mqtt channel only once', function (done) {
        var onMessage = function () { return null; };
        pubSub.subscribe('Posts', onMessage).then(function (id1) {
            return pubSub.subscribe('Posts', onMessage)
                .then(function (id2) { return [id1, id2]; });
        }).then(function (subIds) {
            try {
                expect(subIds.length).to.equals(2);
                expect(subscribeSpy.callCount).to.equals(1);
                pubSub.unsubscribe(subIds[0]);
                pubSub.unsubscribe(subIds[1]);
                done();
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('can have multiple subscribers and all will be called when a message is published to this channel', function (done) {
        var unSubIds = [];
        var callCount = 0;
        var onMessageSpy = simple_mock_1.spy(function () {
            callCount++;
            if (callCount === 2) {
                pubSub.unsubscribe(unSubIds[0]);
                pubSub.unsubscribe(unSubIds[1]);
                expect(onMessageSpy.callCount).to.equals(2);
                onMessageSpy.calls.forEach(function (call) {
                    expect(call.args).to.have.members(['test']);
                });
                done();
            }
        });
        var subscriptionPromises = [
            pubSub.subscribe('Posts', onMessageSpy),
            pubSub.subscribe('Posts', onMessageSpy),
        ];
        Promise.all(subscriptionPromises).then(function (subIds) {
            try {
                expect(subIds.length).to.equals(2);
                pubSub.publish('Posts', 'test');
                unSubIds = subIds;
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('can publish objects as well', function (done) {
        var unSubId;
        var onMessage = function (message) {
            pubSub.unsubscribe(unSubId);
            try {
                expect(message).to.have.property('comment', 'This is amazing');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        pubSub.subscribe('Posts', onMessage).then(function (subId) {
            try {
                pubSub.publish('Posts', { comment: 'This is amazing' });
                unSubId = subId;
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('throws if you try to unsubscribe with an unknown id', function () {
        return expect(function () { return pubSub.unsubscribe(123); })
            .to.throw('There is no subscription of id "123"');
    });
    it('can use transform function to convert the trigger name given into more explicit channel name', function (done) {
        var triggerTransform = function (trigger, _a) {
            var repoName = _a.repoName;
            return trigger + "." + repoName;
        };
        var pubsub = new mqtt_pubsub_1.MQTTPubSub({
            triggerTransform: triggerTransform,
        });
        var unSubId;
        var validateMessage = function (message) {
            pubsub.unsubscribe(unSubId);
            try {
                expect(message).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        pubsub.subscribe('comments', validateMessage, { repoName: 'graphql-mqtt-subscriptions' }).then(function (subId) {
            pubsub.publish('comments.graphql-mqtt-subscriptions', 'test');
            unSubId = subId;
        });
    });
    it('can subscribe with fullPacketInfo', function (done) {
        var pubSubPacketInfo = new mqtt_pubsub_1.MQTTPubSub({
            includeFullPacketInfo: true,
        });
        var sub;
        var onMessage = function (_a) {
            var topic = _a.topic, payload = _a.payload, parsedMessage = _a.parsedMessage;
            pubSubPacketInfo.unsubscribe(sub);
            try {
                expect(topic).to.equals('Posts');
                expect(payload).not.to.be.an('undefined');
                expect(parsedMessage).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        pubSubPacketInfo.subscribe('Posts', onMessage).then(function (subId) {
            expect(subId).to.be.a('number');
            pubSubPacketInfo.publish('Posts', 'test');
            sub = subId;
        }).catch(function (err) { return done(err); });
    });
    it('allows to change encodings of messages passed through MQTT broker', function (done) {
        var pubsub = new mqtt_pubsub_1.MQTTPubSub({
            parseMessageWithEncoding: 'base64',
        });
        var unSubId;
        var validateMessage = function (message) {
            pubsub.unsubscribe(unSubId);
            try {
                expect(message).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        pubsub.subscribe('comments', validateMessage).then(function (subId) {
            pubsub.publish('comments', 'test');
            unSubId = subId;
        });
    });
    it('allows to QoS for each publish topic', function (done) {
        var pubsub = new mqtt_pubsub_1.MQTTPubSub({
            publishOptions: function (topic) {
                var qos = topic === 'comments' ? 2 : undefined;
                return Promise.resolve({ qos: qos });
            },
        });
        var unSubId;
        var validateMessage = function (message) {
            pubsub.unsubscribe(unSubId);
            try {
                expect(publishSpy.calls[0].args[2].qos).to.equals(2);
                expect(message).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        pubsub.subscribe('comments', validateMessage).then(function (subId) {
            pubsub.publish('comments', 'test');
            unSubId = subId;
        });
    });
    it('allows to set QoS for each topic subscription', function (done) {
        var pubsub = new mqtt_pubsub_1.MQTTPubSub({
            subscribeOptions: function (topic) {
                var qos = topic === 'comments' ? 2 : undefined;
                return Promise.resolve({ qos: qos });
            },
            onMQTTSubscribe: function (id, granted) {
                pubsub.unsubscribe(id);
                try {
                    expect(granted[0].topic).to.equals('comments');
                    expect(granted[0].qos).to.equals(2);
                    done();
                }
                catch (e) {
                    done(e);
                }
            },
        });
        pubsub.subscribe('comments', function () { return null; }).catch(done);
    });
    afterEach('Reset spy count', function () {
        publishSpy.reset();
        subscribeSpy.reset();
        unsubscribeSpy.reset();
    });
    after('Restore mqtt client', function () {
        simple_mock_1.restore();
    });
});
describe('PubSubAsyncIterator', function () {
    it('should expose valid asyncItrator for a specific event', function () {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var eventName = 'test';
        var iterator = pubSub.asyncIterator(eventName);
        expect(iterator).to.exist;
        expect(iterall_1.isAsyncIterable(iterator)).to.be.true;
    });
    it('should trigger event on asyncIterator when published', function (done) {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var eventName = 'test';
        var iterator = pubSub.asyncIterator(eventName);
        iterator.next().then(function (result) {
            expect(result).to.exist;
            expect(result.value).to.exist;
            expect(result.done).to.exist;
            done();
        });
        pubSub.publish(eventName, { test: true });
    });
    it('should not trigger event on asyncIterator when publishing other event', function () {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var eventName = 'test2';
        var iterator = pubSub.asyncIterator('test');
        var triggerSpy = simple_mock_1.spy(function () { return undefined; });
        iterator.next().then(triggerSpy);
        pubSub.publish(eventName, { test: true });
        expect(triggerSpy.callCount).to.equal(0);
    });
    it('register to multiple events', function (done) {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var eventName = 'test2';
        var iterator = pubSub.asyncIterator(['test', 'test2']);
        var triggerSpy = simple_mock_1.spy(function () { return undefined; });
        iterator.next().then(function () {
            triggerSpy();
            expect(triggerSpy.callCount).to.be.gte(1);
            done();
        });
        pubSub.publish(eventName, { test: true });
    });
    it('should not trigger event on asyncIterator already returned', function (done) {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var eventName = 'test';
        var iterator = pubSub.asyncIterator(eventName);
        iterator.next().then(function (result) {
            expect(result).to.exist;
            expect(result.value).to.exist;
            expect(result.value.test).to.equal('word');
            expect(result.done).to.be.false;
        });
        pubSub.publish(eventName, { test: 'word' });
        iterator.next().then(function (result) {
            expect(result).to.exist;
            expect(result.value).not.to.exist;
            expect(result.done).to.be.true;
            done();
        });
        iterator.return();
        pubSub.publish(eventName, { test: true });
    });
});
describe('Wildcards in subscription topic', function () {
    it('should receive message while subscribing to topic containing wildcard', function (done) {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var unSubIds = [];
        var callCount = 0;
        var onMessageSpy = simple_mock_1.spy(function () {
            callCount++;
            if (callCount === 3) {
                pubSub.unsubscribe(unSubIds[0]);
                pubSub.unsubscribe(unSubIds[1]);
                expect(onMessageSpy.callCount).to.equals(3);
                onMessageSpy.calls.forEach(function (call) {
                    expect(call.args).to.have.members(['test']);
                });
                done();
            }
        });
        var subscriptionPromises = [
            pubSub.subscribe('Posts/#', onMessageSpy),
            pubSub.subscribe('Posts/CategoryA', onMessageSpy),
        ];
        Promise.all(subscriptionPromises).then(function (subIds) {
            try {
                expect(subIds.length).to.equals(2);
                pubSub.publish('Posts/CategoryA', 'test');
                pubSub.publish('Posts/CategoryB', 'test');
                unSubIds = subIds;
            }
            catch (e) {
                done(e);
            }
        });
    });
    it('can subscribe to everything with "#" topic', function (done) {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var sub;
        var expectedMessages = ['test0', 'test1', 'test2', 'test3'];
        var messages = [];
        var onMessage = function (message) {
            try {
                if (messages.length === 3) {
                    messages.push(message);
                    expect(messages).to.deep.equal(expectedMessages);
                    pubSub.unsubscribe(sub);
                    done();
                }
                else {
                    messages.push(message);
                }
            }
            catch (e) {
                done(e);
            }
        };
        pubSub.subscribe('#', onMessage).then(function (subId) {
            expect(subId).to.be.a('number');
            pubSub.publish('Posts', 'test0');
            pubSub.publish('Posts/A', 'test1');
            pubSub.publish('Posts/A/B', 'test2');
            pubSub.publish('Posts/A/D/C', 'test3');
            sub = subId;
        }).catch(function (err) { return done(err); });
    });
    it('can subscribe to only specific subset', function (done) {
        var pubSub = new mqtt_pubsub_1.MQTTPubSub();
        var sub;
        var expectedMessages = ['test2', 'test3', 'test4'];
        var messages = [];
        var onMessage = function (message) {
            try {
                if (expectedMessages.indexOf(message) > -1) {
                    if (messages.length === 2) {
                        messages.push(message);
                        expect(messages).to.deep.equal(expectedMessages);
                        pubSub.unsubscribe(sub);
                        done();
                    }
                    else {
                        messages.push(message);
                    }
                }
            }
            catch (e) {
                done(e);
            }
        };
        pubSub.subscribe('Posts/+/D', onMessage).then(function (subId) {
            expect(subId).to.be.a('number');
            pubSub.publish('Posts/A', 'test1');
            pubSub.publish('Posts/B/D', 'test2');
            pubSub.publish('Posts/C/D', 'test3');
            pubSub.publish('Posts/E/D', 'test4');
            pubSub.publish('Posts/F/G', 'test5');
            pubSub.publish('Posts/H/D/I', 'test6');
            pubSub.publish('Posts', 'test7');
            sub = subId;
        }).catch(function (err) { return done(err); });
    });
});
//# sourceMappingURL=tests.js.map