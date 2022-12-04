import assert from 'assert'
import fs from 'fs'
import path from 'path'
import {Store} from './store'
import {Table, TableBuilder, TableManager} from './table'
import {Transaction} from './tx'
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

    constructor(options?: CsvDatabaseOptions) {
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

        let tx = new Transaction(this.path)
        let folder = `${from}-${to}`
        tx.mkdir(folder)
        for (let table of this.tables) {
            let tablebuilder = tm.getTableBuilder(table.name)
            tx.writeFile(path.join(folder, `${table.name}.${this.extension}`), tablebuilder.serialize())
        }
        this.updateHeight(tx, from, to)
        tx.commit()
        open = false
        this.lastCommitted = to
    }

    async advance(height: number): Promise<void> {
        if (this.lastCommitted == height) return
        let tx = new Transaction(this.path)
        this.updateHeight(new Transaction(this.path), height, height)
        tx.commit()
    }

    private updateHeight(tx: Transaction, from: number, to: number): void {
        let statusTable = new TableBuilder({height: types.int}, [{height: to}])
        tx.writeFile(`status.${this.extension}`, statusTable.serialize())
    }
}
