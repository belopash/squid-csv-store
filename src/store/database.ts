import {S3Client} from '@aws-sdk/client-s3'
import assert from 'assert'
import {Chunk} from './chunk'
import {Dialect, dialects} from './dialect'
import {createFS, FS, FSOptions, LocalFS, S3Fs} from './fs'
import {Store} from './store'
import {Table, TableBuilder, TableHeader, TableManager} from './table'
import {types} from './types'

export interface CsvDatabaseOptions {
    dest?: string
    encoding?: BufferEncoding
    extension?: string
    dialect?: Dialect
    chunkSize?: number
    fsOptions?: FSOptions
}

export class CsvDatabase {
    // private path: string
    private encoding: BufferEncoding
    private extension: string
    private chunkSize: number
    private dialect: Dialect
    private lastCommitted = -1

    private chunk: Chunk | undefined
    private fs: FS

    constructor(private tables: Table<any>[], options?: CsvDatabaseOptions) {
        this.extension = options?.extension || 'csv'
        this.encoding = options?.encoding || 'utf-8'
        this.dialect = options?.dialect || dialects.excel
        this.chunkSize = options?.chunkSize || 20
        this.fs = createFS(options?.dest || './data', options?.fsOptions)
    }

    async connect(): Promise<number> {
        if (await this.fs.exist('status.csv')) {
            let rows = await this.fs
                .readFile('status.csv', this.encoding)
                .then((data) => data.split(dialects.excel.lineTerminator))
            assert(rows.length == 3)
            return Number(rows[2])
        } else {
            let statusTable = new TableBuilder({height: types.int}, dialects.excel, [{height: -1}])
            await this.fs.writeFile(`status.${this.extension}`, statusTable.data, this.encoding)
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

        if (height > this.lastCommitted) {
            this.chunk.changeRange({to: height})
            this.lastCommitted = height
        }

        let tx = this.fs.transact(this.chunk.name)
        for (let table of this.tables) {
            let tablebuilder = this.chunk.getTableBuilder(table.name)
            await tx.writeFile(`${table.name}.${this.extension}`, tablebuilder.data, this.encoding)
        }
        await tx.commit()

        let statusTable = new TableBuilder({height: types.int}, dialects.excel, [{height}])
        await this.fs.writeFile(`status.csv`, statusTable.data, this.encoding)

        this.chunk = undefined
    }

    private createChunk(from: number, to: number) {
        return new Chunk(from, to, new Map(this.tables.map((t) => [t.name, new TableBuilder(t.header, this.dialect)])))
    }
}
