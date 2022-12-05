import {assertNotNull} from '@subsquid/substrate-processor'
import assert from 'assert'
import fs from 'fs'
import path from 'path'
import {Chunk} from './chunk'
import {Dialect, dialects} from './dialect'
import {Store} from './store'
import {Table, TableBuilder, TableHeader, TableManager} from './table'
import {Transaction} from './tx'
import {types} from './types'

export interface CsvDatabaseOptions {
    path?: string
    encoding?: BufferEncoding
    extension?: string
    dialect?: Dialect
    chunkSize?: number
}

export class CsvDatabase {
    private path: string
    private encoding: BufferEncoding
    private extension: string
    private chunkSize: number
    private dialect: Dialect
    private lastCommitted = -1

    private chunk: Chunk | undefined

    // private get tableManager() {
    //     this._tableManager =
    //         this._tableManager || new Map(this.tables.map((t) => [t.name, new TableBuilder(t.header, this.dialect)]))
    //     return this._tableManager
    // }

    // private set tableManager() {
    //     this._tableManager =
    //         this._tableManager || new Map(this.tables.map((t) => [t.name, new TableBuilder(t.header, this.dialect)]))
    //     return this._tableManager
    // }

    constructor(private tables: Table<any>[], options?: CsvDatabaseOptions) {
        this.path = path.resolve(options?.path ? options.path : './data')
        this.extension = options?.extension || 'csv'
        this.encoding = options?.encoding || 'utf-8'
        this.dialect = options?.dialect || dialects.excel
        this.chunkSize = options?.chunkSize || 20
    }

    async connect(): Promise<number> {
        let dir = path.join(this.path, 'status.csv')
        if (fs.existsSync(dir)) {
            let rows = fs.readFileSync(dir).toString(this.encoding).split(dialects.excel.lineTerminator)
            assert(rows.length == 3)
            return Number(rows[2])
        } else {
            let tx = new Transaction(this.path)
            this.updateHeight(tx, -1, -1)
            tx.commit()
            return -1
        }
    }

    async close(): Promise<void> {
        this.lastCommitted = -1
    }

    async transact(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let retries = 3
        while (true) {
            try {
                return await this.runTransaction(from, to, cb)
            } catch (e: any) {
                if (retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    private async runTransaction(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let open = true

        if (!this.chunk) {
            this.chunk = this.createChunk(from, to)
        } else {
            this.chunk.changeRange({to: to})
        }

        let store = new Store(() => {
            assert(open, `Transaction was already closed`)
            return this.chunk as Chunk
        })

        try {
            await cb(store)
        } catch (e: any) {
            open = false
            throw e
        }

        open = false
        this.lastCommitted = to
    }

    async advance(height: number): Promise<void> {
        if (!this.chunk || this.chunk.totalSize < this.chunkSize * 1024 * 1024) return

        let tx = new Transaction(this.path)

        if (height > this.lastCommitted) this.chunk.changeRange({to: height})
        tx.mkdir(this.chunk.name)

        for (let table of this.tables) {
            let tablebuilder = this.chunk.getTableBuilder(table.name)
            let fileName = path.join(this.chunk.name, `${table.name}.${this.extension}`)
            tx.writeFile(fileName, tablebuilder.data, {encoding: this.encoding})
        }

        let statusTable = new TableBuilder({height: types.int}, dialects.excel, [{height}])
        tx.writeFile(`status.${this.extension}`, statusTable.data)

        tx.commit()

        this.lastCommitted = height
    }

    private createChunk(from: number, to: number) {
        return new Chunk(from, to, new Map(this.tables.map((t) => [t.name, new TableBuilder(t.header, this.dialect)])))
    }

    private updateHeight(tx: Transaction, from: number, to: number): void {
        let statusTable = new TableBuilder({height: types.int}, dialects.excel, [{height: to}])
        tx.writeFile(`status.${this.extension}`, statusTable.data)
    }
}
