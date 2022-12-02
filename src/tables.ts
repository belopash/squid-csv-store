import {Table, types} from './store'

export const Transfers = new Table('transfers', {
    blockNumber: {name: 'block', type: types.Int},
    timestamp: types.DateTime,
    extrinsicHash: types.Option(types.String),
    from: types.String,
    to: types.String,
    amount: types.BigInt,
})

export const Extrinsics = new Table('extrinsics', {
    block: types.Int,
    timestamp: types.DateTime,
    hash: types.String,
    signer: types.String,
})
