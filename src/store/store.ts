import {assertNotNull} from '@subsquid/substrate-processor'
import {Chunk} from './chunk'
import {Table, TableRecord, TableHeader} from './table'

export class Store {
    constructor(private _tables: () => Chunk) {}

    private get tables() {
        return this._tables()
    }

    write<T extends TableHeader>(table: Table<T>, record: TableRecord<T>): void
    write<T extends TableHeader>(table: Table<T>, records: TableRecord<T>[]): void
    write<T extends TableHeader>(table: Table<T>, records: TableRecord<T> | TableRecord<T>[]): void {
        let builder = this.tables.getTableBuilder(table.name)
        builder.append(records)
    }
}
