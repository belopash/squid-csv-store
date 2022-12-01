import {Type} from './types'

export type TableSchema = Record<string, Type<any>>

export type TableData<TableSchema> = {name: string; shema: TableSchema}

export type TableRow<T extends TableSchema> = {[k in keyof T]: T[k] extends Type<infer R> ? R : never}

export class Table<T extends TableSchema> {
    constructor(private schema: T, private rows: TableRow<T>[] = []) {}

    get header() {
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

    append(rows: TableRow<T> | TableRow<T>[]): void {
        if (Array.isArray(rows)) {
            this.rows.push(...rows)
        } else {
            this.rows.push(rows)
        }
    }

    serialize(options?: unknown) {
        let res = this.header
        return (
            res +
            this.rows
                .map((row) =>
                    Object.entries(this.schema)
                        .map(([column, type]) => type.serialize(row[column]))
                        .join(',')
                )
                .join('\n')
        )
    }
}
