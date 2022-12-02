import * as ss58 from '@subsquid/ss58'
import {lookupArchive} from '@subsquid/archive-registry'
import {BatchContext, BatchProcessorItem, SubstrateBatchProcessor} from '@subsquid/substrate-processor'
import {CsvDatabase, Store, types} from './store'
import {BalancesTransferEvent} from './types/events'
import {Extrinsics, Transfers} from './tables'

const processor = new SubstrateBatchProcessor()
    .setDataSource({
        archive: lookupArchive('kusama', {release: 'FireSquid'}),
    })
    .addEvent('Balances.Transfer', {
        data: {
            event: {
                args: true,
                extrinsic: {
                    hash: true,
                },
            },
        },
    } as const)

let db = new CsvDatabase({
    tables: [Transfers, Extrinsics],
})

processor.run(db, async (ctx) => {
    let transfersData = getTransfers(ctx)

    for (let t of transfersData) {
        ctx.store.write(Transfers, t)
    }
})

interface TransferEvent {
    blockNumber: number
    timestamp: Date
    extrinsicHash?: string
    from: string
    to: string
    amount: bigint
}

type Item = BatchProcessorItem<typeof processor>
type Ctx = BatchContext<unknown, Item>

function getTransfers(ctx: Ctx): TransferEvent[] {
    let transfers: TransferEvent[] = []
    for (let block of ctx.blocks) {
        for (let item of block.items) {
            if (item.name == 'Balances.Transfer') {
                let e = new BalancesTransferEvent(ctx, item.event)
                let rec: {from: Uint8Array; to: Uint8Array; amount: bigint}
                if (e.isV1020) {
                    let [from, to, amount] = e.asV1020
                    rec = {from, to, amount}
                } else if (e.isV1050) {
                    let [from, to, amount] = e.asV1050
                    rec = {from, to, amount}
                } else if (e.isV9130) {
                    rec = e.asV9130
                } else {
                    throw new Error('Unsupported spec')
                }

                transfers.push({
                    blockNumber: block.header.height,
                    timestamp: new Date(block.header.timestamp),
                    extrinsicHash: item.event.extrinsic?.hash,
                    from: ss58.codec('kusama').encode(rec.from),
                    to: ss58.codec('kusama').encode(rec.to),
                    amount: rec.amount,
                })
            }
        }
    }
    return transfers
}
