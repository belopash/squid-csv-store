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
    private records: string[] = []

    constructor(private header: TableHeader, private dialect: Dialect, records: TableRecord<T>[] = []) {
        if (this.dialect.header) {
            let serializedHeader = Object.keys(this.header).join(this.dialect.delimiter) + this.dialect.lineTerminator
            let serializedTypes =
                Object.values(this.header)
                    .map((t) => t.name)
                    .join(this.dialect.delimiter) + this.dialect.lineTerminator
            this.records.push(serializedHeader, serializedTypes)
        }
        this.append(records)
    }

    getSize(encoding: BufferEncoding) {
        return this.records.reduce((size, record) => size + Buffer.byteLength(record, encoding), 0)
    }

    getTable() {
        return this.records.join('')
    }

    append(records: TableRecord<T> | TableRecord<T>[]): void {
        records = Array.isArray(records) ? records : [records]
        for (let record of records) {
            let serializedRecord = this.serializeRecord(record) + this.dialect.lineTerminator
            this.records.push(serializedRecord)
        }
    }

    private serializeRecord(record: TableRecord<T>) {
        return Object.entries(this.header)
            .map(([field, fieldData]) => fieldData.serialize(record[field as keyof typeof record], this.dialect))
            .join(this.dialect.delimiter)
    }
}
