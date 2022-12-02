import {BigDecimal as BigDecimal_} from '@subsquid/big-decimal'
import {toHex} from '@subsquid/util-internal-hex'
import {assert} from 'console'

export class Type<T> {
    readonly name: string
    readonly serialize: (value: T) => string

    constructor(options: {name: string; serialize: (value: T) => string}) {
        this.name = options.name
        this.serialize = options.serialize
    }
}

let String = new Type<string>({
    name: 'string',
    serialize(value: string) {
        return value
    },
})

let Int = new Type<number>({
    name: 'int',
    serialize(value: number) {
        assert(Number.isInteger(value))
        return value.toString()
    },
})

let Float = new Type<number>({
    name: 'float',
    serialize(value: number) {
        return value.toString()
    },
})

let BigInt = new Type<bigint>({
    name: 'bigint',
    serialize(value: bigint) {
        return value.toString()
    },
})

let BigDecimal = new Type<BigDecimal_>({
    name: 'bigdecimal',
    serialize(value: BigDecimal_) {
        return value.toString()
    },
})

let Boolean = new Type<boolean>({
    name: 'boolean',
    serialize(value: boolean) {
        return value.toString()
    },
})

let Bytes = new Type<Uint8Array>({
    name: 'bytes',
    serialize(value: Uint8Array) {
        return toHex(value)
    },
})

let DateTime = new Type<Date>({
    name: 'datetime',
    serialize(value: Date) {
        return value.toISOString()
    },
})

let Array = <T>(itemType: Type<T>): Type<T[]> => {
    return new Type({
        name: `array<${itemType.name}>`,
        serialize(value: T[], seperator = '|') {
            return value.map((i) => itemType.serialize(i)).join(seperator)
        },
    })
}

let Option = <T>(type: Type<T>): Type<T | null | undefined> => {
    return new Type({
        name: `option<${type.name}>`,
        serialize(value: T | null | undefined) {
            return value != null ? type.serialize(value) : 'null'
        },
    })
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
    Array,
    Option,
}
