// import type {Connection, EntityManager} from "typeorm"
// import type {IsolationLevel} from "./database"

import path from 'path'
import fs from 'fs'

export class Transaction {
    private tempPath: string
    private path: string

    constructor(dir: string) {
        this.path = path.resolve(dir)

        let tempDir = `${path.basename(this.path)}-temp-${Date.now()}`
        this.tempPath = path.join(path.dirname(this.path), tempDir)

        fs.mkdirSync(this.tempPath, {recursive: true})
    }

    mkdir(name: string) {
        try {
            fs.mkdirSync(path.join(this.tempPath, name), {recursive: true})
        } catch (error) {
            this.rollback(error)
        }
    }

    writeFile(name: string, content: string) {
        try {
            const filePath = path.join(this.tempPath, name)
            fs.writeFileSync(filePath, content)
        } catch (error: any) {
            this.rollback(error)
        }
    }

    commit() {
        try {
            this.mergeDirs(this.tempPath, this.path)
            fs.rmSync(this.tempPath, {recursive: true, force: true})
        } catch (error) {
            this.rollback(error)
        }
    }

    rollback(error: any) {
        fs.rmSync(this.tempPath, {recursive: true, force: true})
        throw error
    }

    mergeDirs(src: string, dst: string) {
        if (!fs.existsSync(path.resolve(dst))) {
            fs.mkdirSync(dst)
        }

        const files = fs.readdirSync(src)
        for (let file of files) {
            const srcFile = path.resolve(path.join(src, file))
            const dstFile = path.resolve(path.join(dst, file))

            const stats = fs.lstatSync(srcFile)
            if (stats.isDirectory()) {
                this.mergeDirs(srcFile, dstFile)
            } else {
                if (fs.existsSync(dstFile)) {
                    fs.rmSync(dstFile)
                }
                fs.renameSync(srcFile, dstFile)
            }
        }
    }
}
