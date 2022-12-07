import {Nullable, Table, types} from '@subsquid/csv-store'

export const Transfers = new Table('transfers', {
    blockNumber: types.int,
    timestamp: types.datetime,
    extrinsicHash: Nullable(types.string),
    from: types.string,
    to: types.string,
    amount: types.bigint,
})

export const Extrinsics = new Table('extrinsics', {
    block: types.int,
    timestamp: types.datetime,
    hash: types.string,
    signer: types.string,
})
