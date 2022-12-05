import {assertNotNull} from '@subsquid/substrate-processor'
import {TableBuilder, TableHeader} from './table'

export class Chunk {
    constructor(private from: number, private to: number, private tables: Map<string, TableBuilder<any>>) {}

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tables.get(name), `Table ${name} does not exist`)
    }

    changeRange(range: {from?: number; to?: number}) {
        if (range.from) this.from = range.from
        if (range.to) this.to = range.to
    }

    get totalSize() {
        let total = 0
        for (let table of this.tables.values()) {
            total += Buffer.byteLength(table.data)
        }
        return total
    }

    get name() {
        return `${this.from}-${this.to}`
    }
}
