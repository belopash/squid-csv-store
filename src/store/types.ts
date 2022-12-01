import {BigDecimal as BigDecimal_} from '@subsquid/big-decimal'
import {toHex} from '@subsquid/util-internal-hex'
import {assert} from 'console'

export interface Type<T> {
    name: string
    serialize: (value: T) => string
}

let String: Type<string> = {
    name: 'string',
    serialize(value: string) {
        return value
    },
}

let Int: Type<number> = {
    name: 'int',
    serialize(value: number) {
        assert(Number.isInteger(value))
        return value.toString()
    },
}

let Float: Type<number> = {
    name: 'float',
    serialize(value: number) {
        return value.toString()
    },
}

let BigInt: Type<bigint> = {
    name: 'bigint',
    serialize(value: bigint) {
        return value.toString()
    },
}

let BigDecimal: Type<BigDecimal_> = {
    name: 'bigdecimal',
    serialize(value: BigDecimal_) {
        return value.toString()
    },
}

let Boolean: Type<boolean> = {
    name: 'boolean',
    serialize(value: boolean) {
        return value.toString()
    },
}

let Bytes: Type<Uint8Array> = {
    name: 'bytes',
    serialize(value: Uint8Array) {
        return toHex(value)
    },
}

let DateTime: Type<Date> = {
    name: 'datetime',
    serialize(value: Date) {
        return value.toISOString()
    },
}

export let types = {
    String,
    Int,
    Float,
    BigInt,
    BigDecimal,
    Bytes,
    DateTime,
    Boolean,
}
