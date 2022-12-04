import {BigDecimal as BigDecimal} from '@subsquid/big-decimal'
import {toHex} from '@subsquid/util-internal-hex'
import {toJSON} from '@subsquid/util-internal-json'
import {assert} from 'console'

export class Type<T> {
    readonly name: string
    readonly serialize: (value: T) => string

    constructor(options: {name: string; serialize: (value: T) => string}) {
        this.name = options.name
        this.serialize = options.serialize
    }
}

export let StringType = new Type<string>({
    name: 'string',
    serialize(value: string) {
        return value
    },
})

export let IntType = new Type<number>({
    name: 'int',
    serialize(value: number) {
        assert(Number.isInteger(value))
        return value.toString()
    },
})

export let FloatType = new Type<number>({
    name: 'float',
    serialize(value: number) {
        return value.toString()
    },
})

export let BigIntType = new Type<bigint>({
    name: 'bigint',
    serialize(value: bigint) {
        return value.toString()
    },
})

export let BigDecimalType = new Type<BigDecimal>({
    name: 'bigdecimal',
    serialize(value: BigDecimal) {
        return value.toString()
    },
})

export let BooleanType = new Type<boolean>({
    name: 'boolean',
    serialize(value: boolean) {
        return value.toString()
    },
})

export let BytesType = new Type<Uint8Array>({
    name: 'bytes',
    serialize(value: Uint8Array) {
        return toHex(value)
    },
})

export let DateTimeType = new Type<Date>({
    name: 'datetime',
    serialize(value: Date) {
        return value.toISOString()
    },
})

export let JSONType = new Type<any>({
    name: 'json',
    serialize(value: any) {
        return JSON.stringify(toJSON(value))
    },
})

export let ArrayType = <T>(itemType: Type<T>): Type<T[]> => {
    return new Type({
        name: `array<${itemType.name}>`,
        serialize(value: T[], seperator = '|') {
            return value.map((i) => itemType.serialize(i)).join(seperator)
        },
    })
}

export let Nullable = <T>(type: Type<T>): Type<T | null | undefined> => {
    return new Type({
        name: `nullable<${type.name}>`,
        serialize(value: T | null | undefined) {
            return value == null ? 'null' : type.serialize(value)
        },
    })
}

export let types = {
    string: StringType,
    int: IntType,
    float: FloatType,
    bigint: BigIntType,
    bigdecimal: BigDecimalType,
    butes: BytesType,
    datetime: DateTimeType,
    boolean: BooleanType,
}
