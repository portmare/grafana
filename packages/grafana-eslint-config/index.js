'use strict';

module.exports = {
  'overrides': [
    {
      'extends': ['./js.js'],
      'files': ['*.js']
    },
    {
      'extends': ['./ts.js'],
      'files': ['*.ts']
    },
    {
      'extends': ['./tsx.js'],
      'files': ['*.tsx']
    }
  ]
};
