import {Table, types} from '@subsquid/csv-store'

export const Transfers = new Table('transfers', {
    blockNumber: types.number,
    timestamp: types.timestamp,
    extrinsicHash: {type: types.string, nullable: true},
    from: types.string,
    to: types.string,
    amount: types.bigint,
})

export const Extrinsics = new Table('extrinsics', {
    blockNumber: types.number,
    timestamp: types.timestamp,
    hash: types.string,
    signer: types.string,
})
