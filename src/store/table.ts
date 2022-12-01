import {Type} from './types'

export type TableSchema = Record<string, Type<any>>

export type TableData<TableSchema> = {name: string; shema: TableSchema}

export type TableRow<T extends TableSchema> = {[k in keyof T]: T[k] extends Type<infer R> ? R : never}

export class Table<T extends TableSchema> {
    constructor(private schema: T, private rows: TableRow<T>[] = []) {}

    get header() {
        let res = ''
        for (let column of Object.keys(this.schema)) {
            res += column
            res += ','
        }
        res += '\n'
        for (let column of Object.keys(this.schema)) {
            res += this.schema[column].name
            res += ','
        }
        res += '\n'
        return res
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
        for (let row of this.rows) {
            for (let column of Object.keys(this.schema)) {
                res += this.schema[column].serialize(row[column])
                res += ','
            }
            res += '\n'
        }
        return res.slice(0, res.length - 1)
    }
}
