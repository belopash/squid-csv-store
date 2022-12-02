import {Table, TableRecord, TableHeader, TableManager} from './table'

export class Store {
    constructor(private tm: () => TableManager) {}

    write<T extends TableHeader>(table: Table<T>, record: TableRecord<T>): void
    write<T extends TableHeader>(table: Table<T>, records: TableRecord<T>[]): void
    write<T extends TableHeader>(table: Table<T>, records: TableRecord<T> | TableRecord<T>[]): void {
        let builder = this.tm().getTableBuilder(table.name)
        builder.append(records)
    }
}
