import {assertNotNull} from '@subsquid/substrate-processor'
import {Dialect} from './dialect'
import {Type} from './types'

export type RecordField<T> = Type<T>

export type TableHeader = {
    [k: string]: RecordField<any>
}

export class Table<T extends TableHeader> {
    constructor(readonly name: string, readonly header: T) {}
}

type ExcludeOptionKeys<T> = {
    [p in keyof T]: T[p] extends RecordField<infer R> ? (null extends R ? never : T[p]) : never
}[keyof T]

export type TableRecord<T extends TableHeader> = {
    [k in keyof Pick<T, ExcludeOptionKeys<T>>]: T[k] extends RecordField<infer R> ? R : never
} & {
    [k in keyof Omit<T, ExcludeOptionKeys<T>>]?: T[k] extends RecordField<infer R> ? R : never
}

export class TableBuilder<T extends TableHeader> {
    private _data: string = ''

    constructor(private schema: TableHeader, private dialect: Dialect, private records: TableRecord<T>[] = []) {
        if (dialect.header) {
            this._data += Object.keys(this.schema).join(',') + this.dialect.lineTerminator
            this._data +=
                Object.values(this.schema)
                    .map((t) => t.name)
                    .join(this.dialect.delimiter) + this.dialect.lineTerminator
        }
    }

    get data(): string {
        if (this.records.length > 0) {
            this._data += this.records
                .map((record) =>
                    Object.entries(this.schema)
                        .map(([field, fieldData]) =>
                            fieldData.serialize(record[field as keyof typeof record], this.dialect)
                        )
                        .join(this.dialect.delimiter)
                )
                .join(this.dialect.lineTerminator)
            this.records = []
        }
        return this._data
    }

    append(records: TableRecord<T> | TableRecord<T>[]): void {
        if (Array.isArray(records)) {
            this.records.push(...records)
        } else {
            this.records.push(records)
        }
    }

    clear() {
        this._data = ''
        if (this.dialect.header) {
            this._data += Object.keys(this.schema).join(',') + this.dialect.lineTerminator
            this._data +=
                Object.values(this.schema)
                    .map((t) => t.name)
                    .join(this.dialect.delimiter) + this.dialect.lineTerminator
        }
    }
}

export class TableManager {
    private tables: Map<string, TableBuilder<any>>

    constructor(tables: Table<any>[], dialect: Dialect) {
        this.tables = new Map(tables.map((t) => [t.name, new TableBuilder(t.header, dialect)]))
    }

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tables.get(name), `Table ${name} does not exist`)
    }

    get totalSize() {
        let total = 0
        for (let table of this.tables.values()) {
            total += Buffer.byteLength(table.data)
        }
        return total
    }
}
