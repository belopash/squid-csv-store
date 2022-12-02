import {assertNotNull} from '@subsquid/substrate-processor'
import {Type} from './types'

export type RecordField<T> = Type<T> | {name: string; type: Type<T>}

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
    constructor(private schema: TableHeader, private rows: TableRecord<T>[] = []) {}

    private get header() {
        let res = Object.keys(this.schema).join(',')
        return (
            res +
            '\n' +
            Object.keys(this.schema)
                .map((k) => this.schema[k].name)
                .join(',') +
            '\n'
        )
    }

    append(rows: TableRecord<T> | TableRecord<T>[]): void {
        if (Array.isArray(rows)) {
            this.rows.push(...rows)
        } else {
            this.rows.push(rows)
        }
    }

    serialize(options?: unknown): string {
        let res = this.header
        return (
            res +
            this.rows
                .map((row) =>
                    Object.entries(this.schema)
                        .map(([field, fieldData]) => {
                            let type = fieldData instanceof Type ? fieldData : fieldData.type
                            return type.serialize(row[field as keyof typeof row])
                        })
                        .join(',')
                )
                .join('\n')
        )
    }
}

export class TableManager {
    private tables: Map<string, TableBuilder<any>>

    constructor(tables: Table<any>[]) {
        this.tables = new Map(tables.map((t) => [t.name, new TableBuilder(t.header)]))
    }

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tables.get(name), `Table ${name} does not exist`)
    }
}
