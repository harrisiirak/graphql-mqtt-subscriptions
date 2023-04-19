"use strict";
var __spreadArrays = (this && this.__spreadArrays) || function () {
    for (var s = 0, i = 0, il = arguments.length; i < il; i++) s += arguments[i].length;
    for (var r = Array(s), k = 0, i = 0; i < il; i++)
        for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
            r[k] = a[j];
    return r;
};
Object.defineProperty(exports, "__esModule", { value: true });
var chai = require("chai");
var chaiAsPromised = require("chai-as-promised");
var graphql_1 = require("graphql");
var graphql_subscriptions_1 = require("graphql-subscriptions");
var mqtt_1 = require("mqtt");
var mqtt_pubsub_1 = require("../mqtt-pubsub");
chai.use(chaiAsPromised);
var expect = chai.expect;
var assert = chai.assert;
var schema = new graphql_1.GraphQLSchema({
    query: new graphql_1.GraphQLObjectType({
        name: 'Query',
        fields: {
            testString: {
                type: graphql_1.GraphQLString,
                resolve: function () {
                    return 'works';
                },
            },
        },
    }),
    subscription: new graphql_1.GraphQLObjectType({
        name: 'Subscription',
        fields: {
            testSubscription: {
                type: graphql_1.GraphQLString,
                resolve: function (root) {
                    return root;
                },
            },
            testFilter: {
                type: graphql_1.GraphQLString,
                resolve: function (_, _a) {
                    var filterBoolean = _a.filterBoolean;
                    return filterBoolean ? 'goodFilter' : 'badFilter';
                },
                args: {
                    filterBoolean: { type: graphql_1.GraphQLBoolean },
                },
            },
            testFilterMulti: {
                type: graphql_1.GraphQLString,
                resolve: function (_, _a) {
                    var filterBoolean = _a.filterBoolean;
                    return filterBoolean ? 'goodFilter' : 'badFilter';
                },
                args: {
                    filterBoolean: { type: graphql_1.GraphQLBoolean },
                    a: { type: graphql_1.GraphQLString },
                    b: { type: graphql_1.GraphQLInt },
                },
            },
            testChannelOptions: {
                type: graphql_1.GraphQLString,
                resolve: function (root) {
                    return root;
                },
                args: {
                    repoName: { type: graphql_1.GraphQLString },
                },
            },
        },
    }),
});
var mqttClient = mqtt_1.connect('mqtt://localhost');
var subManager = new graphql_subscriptions_1.SubscriptionManager({
    schema: schema,
    setupFunctions: {
        'testFilter': function (_, _a) {
            var filterBoolean = _a.filterBoolean;
            return {
                'Filter1': { filter: function (root) { return root.filterBoolean === filterBoolean; } },
            };
        },
        'testFilterMulti': function () {
            return {
                'Trigger1': { filter: function () { return true; } },
                'Trigger2': { filter: function () { return true; } },
            };
        },
    },
    pubsub: new mqtt_pubsub_1.MQTTPubSub({
        client: mqttClient,
    }),
});
describe('SubscriptionManager', function () {
    before('wait for connection', function (done) {
        mqttClient.on('connect', function () {
            done();
        });
    });
    it('throws an error if query is not valid', function () {
        var query = 'query a{ testInt }';
        var callback = function () { return null; };
        return expect(subManager.subscribe({ query: query, operationName: 'a', callback: callback }))
            .to.eventually.be.rejectedWith('Subscription query has validation errors');
    });
    it('rejects subscriptions with more than one root field', function () {
        var query = 'subscription X{ a: testSubscription, b: testSubscription }';
        var callback = function () { return null; };
        return expect(subManager.subscribe({ query: query, operationName: 'X', callback: callback }))
            .to.eventually.be.rejectedWith('Subscription query has validation errors');
    });
    it('can subscribe with a valid query and gets a subId back', function () {
        var query = 'subscription X{ testSubscription }';
        var callback = function () { return null; };
        subManager.subscribe({ query: query, operationName: 'X', callback: callback }).then(function (subId) {
            expect(subId).to.be.a('number');
            subManager.unsubscribe(subId);
        });
    });
    it.only('can subscribe with a valid query and get the root value', function (done) {
        var query = 'subscription X{ testSubscription }';
        var callback = function (err, payload) {
            if (err) {
                done(err);
            }
            try {
                expect(payload.data.testSubscription).to.equals('good');
            }
            catch (e) {
                done(e);
                return;
            }
            done();
        };
        subManager.subscribe({ query: query, operationName: 'X', callback: callback }).then(function (subId) {
            subManager.publish('testSubscription', 'good');
            setTimeout(function () {
                subManager.unsubscribe(subId);
            }, 10000);
        });
    });
    it('can use filter functions properly', function (done) {
        var query = "subscription Filter1($filterBoolean: Boolean){\n       testFilter(filterBoolean: $filterBoolean)\n      }";
        var callback = function (err, payload) {
            if (err) {
                done(err);
            }
            try {
                expect(payload.data.testFilter).to.equals('goodFilter');
            }
            catch (e) {
                done(e);
                return;
            }
            done();
        };
        subManager.subscribe({
            query: query,
            operationName: 'Filter1',
            variables: { filterBoolean: true },
            callback: callback,
        }).then(function (subId) {
            subManager.publish('Filter1', { filterBoolean: false });
            subManager.publish('Filter1', { filterBoolean: true });
            setTimeout(function () {
                subManager.unsubscribe(subId);
            }, 20);
        });
    });
    it('can subscribe to more than one trigger', function (done) {
        var triggerCount = 0;
        var query = "subscription multiTrigger($filterBoolean: Boolean, $uga: String){\n       testFilterMulti(filterBoolean: $filterBoolean, a: $uga, b: 66)\n      }";
        var callback = function (err, payload) {
            if (err) {
                done(err);
            }
            try {
                expect(payload.data.testFilterMulti).to.equals('goodFilter');
                triggerCount++;
            }
            catch (e) {
                done(e);
                return;
            }
            if (triggerCount === 2) {
                done();
            }
        };
        subManager.subscribe({
            query: query,
            operationName: 'multiTrigger',
            variables: { filterBoolean: true, uga: 'UGA' },
            callback: callback,
        }).then(function (subId) {
            subManager.publish('NotATrigger', { filterBoolean: false });
            subManager.publish('Trigger1', { filterBoolean: true });
            subManager.publish('Trigger2', { filterBoolean: true });
            setTimeout(function () {
                subManager.unsubscribe(subId);
            }, 30);
        });
    });
    it('can unsubscribe', function (done) {
        var query = 'subscription X{ testSubscription }';
        var callback = function (err) {
            if (err) {
                done(err);
            }
            try {
                assert(false);
            }
            catch (e) {
                done(e);
                return;
            }
            done();
        };
        subManager.subscribe({ query: query, operationName: 'X', callback: callback }).then(function (subId) {
            subManager.unsubscribe(subId);
            subManager.publish('testSubscription', 'bad');
            setTimeout(done, 30);
        });
    });
    it('throws an error when trying to unsubscribe from unknown id', function () {
        expect(function () { return subManager.unsubscribe(123); })
            .to.throw('undefined');
    });
    it('calls the error callback if there is an execution error', function (done) {
        var query = "subscription X($uga: Boolean!){\n      testSubscription  @skip(if: $uga)\n    }";
        var callback = function (err, payload) {
            try {
                expect(payload).to.be.undefined;
                expect(err.message).to.equals('Variable "$uga" of required type "Boolean!" was not provided.');
            }
            catch (e) {
                done(e);
                return;
            }
            done();
        };
        subManager.subscribe({ query: query, operationName: 'X', callback: callback }).then(function (subId) {
            subManager.publish('testSubscription', 'good');
            setTimeout(function () {
                subManager.unsubscribe(subId);
            }, 40);
        });
    });
    it('can use transform function to convert the trigger name given into more explicit channel name', function (done) {
        var triggerTransform = function (trigger, _a) {
            var path = _a.path;
            return __spreadArrays([trigger], path).join('.');
        };
        var pubsub = new mqtt_pubsub_1.MQTTPubSub({
            triggerTransform: triggerTransform,
        });
        var subManager2 = new graphql_subscriptions_1.SubscriptionManager({
            schema: schema,
            setupFunctions: {
                testChannelOptions: function (_, _a) {
                    var repoName = _a.repoName;
                    return ({
                        comments: {
                            channelOptions: { path: [repoName] },
                        },
                    });
                },
            },
            pubsub: pubsub,
        });
        var callback = function (err, payload) {
            if (err) {
                done(err);
            }
            try {
                expect(payload.data.testChannelOptions).to.equals('test');
                done();
            }
            catch (e) {
                done(e);
            }
        };
        var query = "\n      subscription X($repoName: String!) {\n        testChannelOptions(repoName: $repoName)\n      }\n    ";
        var variables = { repoName: 'graphql-redis-subscriptions' };
        subManager2.subscribe({ query: query, operationName: 'X', variables: variables, callback: callback }).then(function (subId) {
            pubsub.publish('comments.graphql-redis-subscriptions', 'test');
            setTimeout(function () { return pubsub.unsubscribe(subId); }, 4);
        });
    });
});
//# sourceMappingURL=integration-tests.js.map