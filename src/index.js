import datastore from '@google-cloud/datastore';
import makeDebug from 'debug';
import Proto from 'uberproto';
import { NotFound } from 'feathers-errors';
const debug = makeDebug('feathers-datastore');

const MAX_INDEX_SIZE = 1500;

class Datastore {
  constructor(options = {}) {
    this.store = datastore({ projectId: options.projectId });

    this.id = options.id || 'id';
    this.kind = options.kind;
    this.events = options.events;
    this.autoIndex = options.autoIndex || false;

    // NOTE: This isn't nice, but it's the only way to give internal methods full
    //  unrestricted (no hooks) access to all methods
    [
      'find',
      'get',
      'create',
      'update',
      'patch',
      'remove'
    ].forEach(method => {
      this[method] = (...args) => this[`_${method}`](...args);
    });
  }

  extend(obj) {
    return Proto.extend(obj, this);
  }

  _get(id, params) {
    let key = this.makeKey(id, params);
    return this.store.get(key)
      .then(([ entity ]) => entity)
      .then(entity => this.entityToPlain(entity, true))
      .then(entity => {
        if (!entity) {
          throw new NotFound(`No record found for id '${id}'`);
        }

        return entity;
      });
  }

  _create(data, params = {}) {
    let entities,
        key;

    if (data.hasOwnProperty(this.id)) {
      key = this.makeKey(data[this.id], params);
    } else {
      key = this.makeKey(undefined, params);
    }

    entities = { key, data };

    // Normalize
    if (Array.isArray(data)) {
      entities = data.map(data => ({ key, data }));
    }

    // Convert entities to explicit format, to allow for indexing
    entities = this.makeExplicitEntity(entities, params);

    return this.store.insert(entities)
      .then(() => entities)
      .then(entity => this.entityToPlain(entity));
  }

  _update(id, data, params = {}) {
    let key = this.makeKey(id, params),
        entity = { key, data },
        { query = {} } = params,
        method = query.create ? 'upsert' : 'update';

    entity = this.makeExplicitEntity(entity, params);

    return this.store[method](entity)
      .then(() => entity)
      .then(entity => this.entityToPlain(entity))
      .catch(err => {
        // NOTE: Updating a not found entity will result in a bad request, rather than
        //  a not found, this gets around that, though in future should be made more
        //  secure
        if (err.code === 400 && err.message === 'no entity to update') {
          throw new NotFound(`No record found for id \'${id}'`);
        }

        throw err;
      });
  }

  _patch(id, data, params) {

    return Promise.resolve()
      .then(() => id ? this._get(id, params) : this._find(params))
      .then(results => {
        let entities,
            makeNewEntity = (current, update) => {
              return {
                key: this.makeKey(current[ this.id ], params),
                data: Object.assign({}, current, update)
              };
            };

        if (Array.isArray(results)) {
          entities = results.map(current => makeNewEntity(current, data));
        } else {
          entities = makeNewEntity(results, data);
        }

        entities = this.makeExplicitEntity(entities, params);

        return this.store.update(entities)
          .then(() => entities);
      })
      .then(entity => this.entityToPlain(entity));
  }

  _find(params = {}) {
    params.query = params.query || {};

    let { ancestor, namespace, kind = this.kind, ...query } = params.query,
        dsQuery = this.store.createQuery(namespace, kind),
        filters;

    if (ancestor) {
      let ancestorKey = this.makeKey(ancestor, params);
      dsQuery = dsQuery.hasAncestor(ancestorKey);
    }

    filters = Object.entries(query)
      .reduce((filters, [key, value]) => {
        const opMap = {
          $gt: '>',
          $gte: '>=',
          $lt: '<',
          $lte: '<=',
          '=': '='
        };

        let special;

        // Normalize
        if (typeof value !== 'object' || value instanceof this.store.key().constructor || value === null) {
          value = { '=': value };
        }

        special = Object.entries(value)
          .filter(([ op ]) => opMap[op])
          .map(([ op, val ]) => {
            // Try convert it into a number
            if (typeof val === 'string') {
              let valAsNum = parseFloat(val);

              if (!isNaN(valAsNum)) {
                // Its a number, assign it to original val
                val = valAsNum;
              }
            }

            return [ key, opMap[op], val ];
          });

        return [...filters, ...special];
      }, []);

    dsQuery = filters.reduce((q, filter) => q.filter(...filter), dsQuery);

    return dsQuery.run()
      .then(([ e ]) => e)
      .then(entity => this.entityToPlain(entity, true))
      .then(data => {
        if (ancestor) {
          return data.filter(({ id }) => id !== ancestor);
        }

        return data;
      });
  }

  _remove(id, params) {
    return Promise.resolve()
      .then(() => id ? this._get(id, params) : this._find(params))
      .then(results => {
        let keys;

        if (Array.isArray(results)) {
          keys = results.map(({ [ this.id ]: id }) => this.makeKey(id, params));
        } else {
          keys = this.makeKey(results[ this.id ], params);
        }

        return this.store.delete(keys)
          .then(() => results);
      });
  }

  makeKey(id, params = {}) {
    let { query = {} } = params,
        key;

    if (Array.isArray(id) || typeof id === 'object') {
      key = this.store.key(id);
    } else {
      // Try fetching a number
      let idAsNum = parseInt(id);
      if (!isNaN(idAsNum)) {
        id = idAsNum;
      }

      key = this.store.key([ this.kind, id ]);
    }

    if (query.namespace) {
      key.namespace = query.namespace;
    }

    return key;
  }

  entityToPlain(entity, alreadyFlat = false) {
    const ID_PROP = this.id;
    let data;

    if (Array.isArray(entity)) {
      return entity.map(e => this.entityToPlain(e, alreadyFlat));
    }

    if (!entity) {
      return entity;
    }

    data = alreadyFlat ? entity : entity.data;

    if (Array.isArray(data)) {
      // In explicit syntax, should deconstruct
      data = data.reduce((flat, { name, value }) => {
        flat[name] = value;
        return flat;
      }, {});
    }

    return Object.assign({}, data, { [ ID_PROP ]: Datastore.getKey(entity).path.slice(-1)[0] });
  }

  makeExplicitEntity(entity, options = {}) {
    const { dontIndex = [], autoIndex = this.autoIndex } = options.query || {};

    function isBig(value) {
      let valueType = typeof value;

      if (valueType === 'string' || value instanceof Buffer) {
        return Buffer.from(value).length > MAX_INDEX_SIZE;
      } else if (valueType === 'object' && value !== null) {
        // Must be an object, recursively build response
        return Object.keys(value)
          .map(key => value[key])
          .some(isBig);
      }

      // Number, boolean, undefined and null we either don't care about or are
      //  guaranteed < 1500
      return false;
    }

    function expandData(data) {
      const toBasicResponse = (name) => ({ name, value: data[name] }),
            addExclusions = (response) => {
              if (dontIndex.includes(response.name)) {
                response.excludeFromIndexes = true;
              } else if (autoIndex) {
                response.excludeFromIndexes = isBig(response.value);
              }

              return response;
            };

      return Object.keys(data)
        .map(toBasicResponse)
        .map(addExclusions);
    }

    if (Array.isArray(entity)) {
      return entity.map(this.makeExplicitEntity, this);
    }

    return {
      key: Datastore.getKey(entity),
      data: expandData(entity.data)
    };
  }

  static getKey(entity) {
    return entity[datastore.KEY] || entity.key;
  }
}

export default function init(options) {
  debug('Initializing feathers-datastore plugin');
  return new Datastore(options);
}

init.Service = Datastore;
