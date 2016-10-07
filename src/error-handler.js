// import errors from 'feathers-errors';

export default function errorHandler(error) {
  let feathersError = error;

  // TODO: Convert all gcloud errors to feathers errors

  throw feathersError;
}
