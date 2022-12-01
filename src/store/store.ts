import {assertNotNull} from '@subsquid/substrate-processor'
import {Table, TableRow, TableSchema} from './table'

export class TableManager {
    private tables: Map<string, Table<any>>

    constructor(tables: Record<string, TableSchema>) {
        this.tables = new Map(Object.entries(tables).map(([n, s]) => [n, new Table(s)]))
    }

    getTable<T extends TableSchema>(name: string): Table<T> | undefined {
        return this.tables.get(name)
    }
}

export class Store<T extends Record<string, TableSchema>> {
    constructor(private tm: () => TableManager) {}

    write<N extends keyof T>(name: N, row: TableRow<T[N]>): void
    write<N extends keyof T>(name: N, rows: TableRow<T[N]>[]): void
    write<N extends keyof T>(name: N, rows: TableRow<T[N]> | TableRow<T[N]>[]): void {
        let table = assertNotNull(this.tm().getTable(name as string))
        table.append(rows)
    }
}
