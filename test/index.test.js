import { expect } from 'chai';
import service from '../src';
import feathers from 'feathers';
import errors from 'feathers-errors';
import { base } from 'feathers-service-tests';
import datastore from '@google-cloud/datastore';

const projectId = 'feathers-test';
const store = datastore({ projectId });

function purge(done) {
  store.createQuery()
    .run((err, entities) => {
      if (err) {
        done(err);
        return;
      }

      let keys = entities.map(service.Service.getKey);
      store.delete(keys, done);
    });
}

describe('feathers-datastore', () => {
  const app = feathers(),
        kind = 'Person',
        people = service({ kind, projectId });

  before((done) => {
    app.use('/datastore', service({
      kind: 'People',
      events: [ 'testing' ],
      projectId
    }));

    app.use('/datastore-alt-id', service({
      kind: 'People',
      id: '_id',
      events: [ 'testing' ],
      projectId
    }));

    purge(done);
  });

  afterEach(purge);

  it('is CommonJS compatible', () => {
    expect(typeof require('../lib')).to.equal('function');
  });

  it('basic functionality', done => {
    expect(typeof service).to.equal('function', 'It worked');
    done();
  });

  it('exposes the Service class', done => {
    expect(service.Service).to.not.equal(undefined);
    done();
  });

  it('take autoIndex as a constructor prop, default to false', () => {
    expect(service({ kind: 'Person', autoIndex: true }).autoIndex).to.equal(true);
    expect(service({ kind: 'Person' }).autoIndex).to.equal(false);
  });

  describe('Service utility tests', () => {
    describe('makeKey', () => {
      it(`should make path like: [kind, id]`, () => {
        let kind = 'Person',
            id = 'Bob',
            key = service({ kind }).makeKey(id);

        expect(key.path).to.deep.equal([ kind, id ]);
      });

      it(`should set namespace if given a namespace`, () => {
        let namespace = 'Melbourne',
            kind = 'Person',
            id = 'Bob',
            key = service({ kind }).makeKey(id, { query: { namespace } });

        expect(key.namespace).to.equal(namespace);
      });
    });
  });

  describe('find', () => {
    it('should be able to filter by parent if needed', () => {
      let bobsChild,
          kind = 'Person',
          people = service({ kind, projectId });

      return Promise.all([
        people.create({ father: people.makeKey('Bob') }),
        people.create({ father: people.makeKey('Graham') })
      ]).then(([{ id }]) => {
        bobsChild = id;
        return people.find({ query: { father: people.makeKey('Bob') } });
      }).then(found => {
        expect(found.length).to.equal(1);
        expect(found[0].id).to.equal(bobsChild);
      });
    });

    it('should be able to filter by null values', () => {
      let kind = 'Person',
          people = service({ kind, projectId }),
          orphan;

      return Promise.all([
        people.create({ name: 'James', father: null })
      ]).then(([{ id }]) => {
        orphan = id;
        return people.find({ query: { father: null } });
      }).then(found => {
        expect(found.length).to.equal(1);
        expect(found[0].id).to.equal(orphan);
      });
    });
  });

  describe('create', () => {
    it('set indexes when creating', () => {
      let data = { id: 'Bob', age: 44, children: 2 },
          params = { query: { dontIndex: ['age'] } };

      return people.create(data, params)
        .then(results => {
          expect(results.age).to.equal(data.age);
          expect(results.children).to.equal(data.children);
        })
        .then(() => people.find({ query: { age: { $lte: 50 } } }))
        .then((results) => {
          expect(results.length).to.equal(0);
        })
        .then(() => people.find({ query: { children: { $lte: 4 } } }))
        .then((results) => {
          expect(results[0]).to.deep.equal(data);
        });
    });

    it('should set id resource prop to id in passed data, iff exists', () => {
      let data = { id: 'Bob', age: 23 };

      return people.create(data)
        .then(() => people.get(data.id))
        .then(person => {
          expect(person).to.deep.equal(data);
        });
    });

    it('should be able to create data with given ids for an array of data', () => {
      let data = [ 'John', 'Matt' ].map(name => ({ id: name, age: 24 }));

      return people.create(data)
        .then(() => people.find())
        .then(found => {
          expect(found).to.deep.equal(data);
        });
    });
  });

  describe('update', () => {
    describe('indexes', () => {
      let data = { id: 'Bob', age: 44, children: 2 },
          params = { query: { dontIndex: ['age'] } },
          id;

      it('set indexes when updating', () => {
        return people.create(data)
          .then(results => {
            id = results.id;
            expect(results.age).to.equal(data.age);
            expect(results.children).to.equal(data.children);
          })
          .then(() => people.update(id, data, params))
          .then(() => people.find({ query: { age: { $lte: 50 } } }))
          .then((results) => {
            expect(results.length).to.equal(0);
          })
          .then(() => people.find({ query: { children: { $lte: 4 } } }))
          .then((results) => {
            expect(results[0]).to.deep.equal(data);
          });
      });
    });

    describe('auto indexing', () => {
      const bigLength = 2000,
            smallLength = 1000;

      let big = Buffer.alloc(bigLength, 'a').toString(),
          small = Buffer.alloc(smallLength, 'a').toString(),
          partialData = { id: 'Bob' },
          data = Object.assign({ big, small }, partialData),
          params = { query: { autoIndex: true } },
          id;

      beforeEach(() => {
        people._defaultAutoIndex = people.autoIndex;
      });

      afterEach(() => {
        people.autoIndex = people._defaultAutoIndex;
      });

      it('should not index lengths > 1500 bytes', () => {
        return people.create(partialData)
          .then(results => {
            id = results.id;
          })
          .then(() => people.update(id, data, params))
          // NOTE: Can't currently do this as can't perform a query of over 1500
          //  (makes sense given cant index over 1500 bytes either) but in future
          //  once $ne is supported, could achieve it via big: { $ne: 'not big' }
          // .then(() => people.find({ query: { big }))
          // .then((results) => {
          //   expect(results.length).to.equal(0);
          // })
          .then(() => people.find({ query: { small } }))
          .then((results) => {
            expect(results[0]).to.deep.equal(data);
          });
      });

      it('should autoIndex if service.autoIndex is true', () => {
        people.autoIndex = true;
        return people.create(data)
          .then(response => {
            expect(response.big).to.equal(big);
          });
      });

      it('should not auto index if not set', () => {
        people.autoIndex = false;
        return people.create(data)
          .then(() => false, () => true)
          .then(errored => {
            expect(errored).to.equal(true);
          });
      });
    });
  });

  describe('patch', () => {
    it('set indexes when patching', () => {
      let data = { id: 'Bob', age: 44, children: 2 },
          params = { query: { dontIndex: ['age'] } },
          id;

      return people.create(data)
        .then(results => {
          id = results.id;
          expect(results.age).to.equal(data.age);
          expect(results.children).to.equal(data.children);
        })
        .then(() => people.update(id, data, params))
        .then(() => people.find({ query: { age: { $lte: 50 } } }))
        .then((results) => {
          expect(results.length).to.equal(0);
        })
        .then(() => people.find({ query: { children: { $lte: 4 } } }))
        .then((results) => {
          expect(results[0]).to.deep.equal(data);
        });
    });
  });

  base(app, errors, 'datastore', 'id');
  base(app, errors, 'datastore-alt-id', '_id');
});
