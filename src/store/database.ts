import assert from 'assert'
import fs from 'fs'
import path from 'path'
import {Store} from './store'
import {Table, TableBuilder, TableManager} from './table'
import {types} from './types'

export interface CsvDatabaseOptions {
    path?: string
    encoding?: BufferEncoding
    extension?: string
    tables: Table<any>[]
}

export class CsvDatabase {
    private path: string
    private encoding: BufferEncoding
    private extension: string
    private lastCommitted = -1
    private tables: Table<any>[]

    constructor(private options?: CsvDatabaseOptions) {
        this.path = path.resolve(options?.path ? options.path : './data')
        this.tables = options?.tables || []
        this.extension = options?.extension || 'csv'
        this.encoding = options?.encoding || 'utf-8'
    }

    async connect(): Promise<number> {
        let dir = path.join(this.path, 'status.csv')
        if (fs.existsSync(dir)) {
            let rows = fs.readFileSync(dir).toString(this.encoding).split('\n')
            assert(rows.length == 3)
            return Number(rows[2])
        } else {
            let statusTable = new TableBuilder({height: types.Int}, [{height: -1}])

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

    async transact(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
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

    private async runTransaction(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
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

        for (let table of this.tables) {
            let tablebuilder = tm.getTableBuilder(table.name)
            fs.writeFileSync(path.join(folder, `${table.name}.${this.extension}`), tablebuilder.serialize(), {
                encoding: this.encoding,
            })
        }

        open = false
        this.lastCommitted = to
    }

    async advance(height: number): Promise<void> {
        await this.updateHeight(height, height)
    }

    private async updateHeight(from: number, to: number): Promise<void> {
        let dir = path.join(this.path, `status.${this.extension}`)
        let statusTable = new TableBuilder({height: types.Int}, [{height: to}])

        if (!fs.existsSync(this.path)) {
            fs.mkdirSync(this.path, {recursive: true})
        }

        fs.writeFileSync(dir, statusTable.serialize())
    }
}
