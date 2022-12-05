import path from 'path'
import fs from 'fs'

export class Transaction {
    private tempPath: string
    private path: string
    private fs = fs.promises

    constructor(dir: string) {
        this.path = path.resolve(dir)

        let tempDir = `${path.basename(this.path)}-temp-${Date.now()}`
        this.tempPath = path.join(path.dirname(this.path), tempDir)

        fs.mkdirSync(this.tempPath, {recursive: true})
    }

    async mkdir(name: string) {
        try {
            await this.fs.mkdir(path.join(this.tempPath, name), {recursive: true})
        } catch (error) {
            await this.rollback(error)
        }
    }

    async writeFile(name: string, content: string, options?: {encoding: BufferEncoding}) {
        try {
            const filePath = path.join(this.tempPath, name)
            await this.fs.writeFile(filePath, content, options)
        } catch (error: any) {
            await this.rollback(error)
        }
    }

    async commit() {
        try {
            await this.mergeDirs(this.tempPath, this.path)
            await this.fs.rm(this.tempPath, {recursive: true, force: true})
        } catch (error) {
            this.rollback(error)
        }
    }

    async rollback(error: any) {
        await this.fs.rm(this.tempPath, {recursive: true, force: true})
        throw error
    }

    private async mergeDirs(src: string, dst: string) {
        if (!fs.existsSync(path.resolve(dst))) {
            await this.fs.mkdir(dst)
        }

        const files = await this.fs.readdir(src)
        for (let file of files) {
            const srcFile = path.resolve(path.join(src, file))
            const dstFile = path.resolve(path.join(dst, file))

            const stats = await this.fs.lstat(srcFile)
            if (stats.isDirectory()) {
                await this.mergeDirs(srcFile, dstFile)
            } else {
                if (fs.existsSync(dstFile)) {
                    await this.fs.rm(dstFile)
                }
                await this.fs.rename(srcFile, dstFile)
            }
        }
    }
}
