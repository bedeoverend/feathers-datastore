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

      let keys = entities.map(({ key }) => key);
      store.delete(keys, done);
    });
}

describe('feathers-datastore', () => {
  const app = feathers();

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
    const kind = 'Person',
          people = service({ kind, projectId });

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

  base(app, errors, 'datastore', 'id');
  base(app, errors, 'datastore-alt-id', '_id');
});
