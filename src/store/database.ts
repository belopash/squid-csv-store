import {createOrmConfig} from '@subsquid/typeorm-config'
import {assertNotNull} from '@subsquid/util-internal'
import assert from 'assert'
import fs from 'fs'
import path from 'path'
import {Store, TableManager} from './store'
import {Table, TableData, TableSchema} from './table'
import {types} from './types'

export interface CsvDatabaseOptions<T extends Record<string, TableSchema>> {
    path?: string
    encoding?: BufferEncoding
    tables: T
}

export class CsvDatabase<T extends Record<string, TableSchema>> {
    protected path: string
    protected encoding: BufferEncoding
    protected lastCommitted = -1
    protected tables: Record<string, TableSchema>

    constructor(protected options?: CsvDatabaseOptions<T>) {
        this.path = path.resolve(options?.path ? options.path : './data')
        this.tables = options?.tables || {}
        this.encoding = options?.encoding || 'utf-8'
    }

    async connect(): Promise<number> {
        let dir = path.join(this.path, 'status.csv')
        if (fs.existsSync(dir)) {
            let rows = fs.readFileSync(dir).toString(this.encoding).split('\n')
            assert(rows.length == 3)
            return Number(rows[2])
        } else {
            let statusTable = new Table({
                height: types.Int,
            })
            statusTable.append({height: -1})

            if (!fs.existsSync(this.path)) {
                fs.mkdirSync(this.path, {recursive: true})
            }

            fs.writeFileSync(dir, statusTable.serialize(), {encoding: this.encoding})
            return -1
        }
    }

    async close(): Promise<void> {
        this.lastCommitted = -1
    }

    async transact(from: number, to: number, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let retries = 3
        while (true) {
            try {
                return await this.runTransaction(from, to, cb)
            } catch (e: any) {
                if (e.code == '40001' && retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    protected async runTransaction(from: number, to: number, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let tm = new TableManager(this.tables)
        let open = true

        let store = new Store(() => {
            assert(open, `Transaction was already closed`)
            return tm
        })

        try {
            await cb(store)
        } catch (e: any) {
            open = false
            throw e
        }

        let folder = path.join(this.path, `${from}-${to}`)

        if (!fs.existsSync(folder)) {
            fs.mkdirSync(folder, {recursive: true})
        }

        for (let tableName of Object.keys(this.tables)) {
            let table = assertNotNull(tm.getTable(tableName))
            fs.writeFileSync(path.join(folder, `${tableName}.csv`), table.serialize(), {encoding: this.encoding})
        }

        open = false
        this.lastCommitted = to
    }

    async advance(height: number): Promise<void> {
        await this.updateHeight(height, height)
    }

    protected async updateHeight(from: number, to: number): Promise<void> {
        let dir = path.join(this.path, 'status.csv')
        let statusTable = new Table(
            {
                height: types.Int,
            },
            [{height: to}]
        )

        if (!fs.existsSync(this.path)) {
            fs.mkdirSync(this.path, {recursive: true})
        }

        fs.writeFileSync(dir, statusTable.serialize())
    }
}
