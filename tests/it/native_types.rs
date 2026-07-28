use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

macro_rules! test_type {
    (
        $(#[$attr:meta])*
        $fn_name:ident($ty:ty $(, $sqlty:literal)?) {
            $($sql:literal == $rust:expr),* $(,)?
        }
    ) => {
        $(#[$attr])*
        #[tokio::test]
        async fn $fn_name() {
            let client = $crate::get_client();

            let mut cursor = client
                .query(concat!(
                    "SELECT c1 FROM Values ("
                    $(,"'c1 ", $sqlty, "', ")?
                    $(, $sql, )","*
                    ")"
                ))
                .fetch_native()
                .expect("error from `.fetch_native()`");

            let block = cursor.next().await
                .expect("error from `cursor.next()`")
                .expect("expected block, got none");

            let mut iter = block["c1"]
                .iter::<$ty>()
                .expect("error from `.iter()`");

            $(
                let expected = $rust;

                let val = iter
                    .next()
                    .expect("expected another value, got none")
                    .unwrap_or_else(|e| panic!("error decoding SQL `{}` as Rust value `{expected:?}`: {e:?}", $sql));

                assert_eq!(val, expected);
            )*

            if let Some(next) = iter.next() {
                panic!("unexpected value: {next:?}");
            }
        }
    };
}

test_type!(test_bool(bool) { "false" == false, "true" == true });

test_type!(test_uint8(u8) { "0" == 0, "1" == 1, "255" == 255 });
test_type!(test_uint16(u16) { "0" == 0, "1" == 1, "255" == 255, "16384" == 16384, "65535" == 65535 });
test_type!(test_uint32(u32) {
    "0" == 0, "1" == 1, "255" == 255, "16384" == 16384, "65535" == 65535,
    "0xFFFF_FFFF" == 0xFFFF_FFFF,
});
test_type!(test_uint64(u64) {
    "0" == 0, "1" == 1, "255" == 255, "16384" == 16384, "65535" == 65535,
    "0xFFFF_FFFF" == 0xFFFF_FFFF, "0xFFFF_FFFF_FFFF_FFFF" == 0xFFFF_FFFF_FFFF_FFFF
});

// Explicit typing is required, otherwise the unsigned values cause an implicit widening
// to the next larger signed type:
//
// `SELECT toTypeName([-1])` => `Array(Int8)`
// `SELECT toTypeName([-1, 1])` => `Array(Int16)`
test_type!(test_int8(i8, "Int8") { "-128" == -128, "-1" == -1, "0" == 0, "1" == 1, "127" == 127 });
test_type!(test_int16(i16, "Int16") {
    "-32768 AS Int16" == -32768, "-128" == -128, "-1" == -1,
    "0" == 0, "1" == 1, "255" == 255, "16384" == 16384, "32767" == 32767
});
test_type!(test_int32(i32, "Int32") {
    "-0x8000_0000" == -0x8000_0000, "-32768" == -32768, "-128" == -128, "-1" == -1,
    "0" == 0, "1" == 1, "255" == 255, "16384" == 16384, "65535" == 65535,
    "0x7FFF_FFFF" == 0x7FFF_FFFF,
});
test_type!(test_int64(i64, "Int64") {
    "-0x8000_0000_0000_0000" == -0x8000_0000_0000_0000, "-0x8000_0000" == -0x8000_0000,
    "-32768" == -32768, "-128" == -128, "-1" == -1,
    "0" == 0, "1" == 1, "255" == 255, "16384" == 16384, "65535" == 65535,
    "0xFFFF_FFFF" == 0xFFFF_FFFF, "0x7FFF_FFFF_FFFF_FFFF" == 0x7FFF_FFFF_FFFF_FFFF
});

test_type!(
    #[allow(clippy::approx_constant)] // we can't be certain the constant for pi is the same
    test_float32(f32, "Float32") {
        // ClickHouse rejects the bare literal but allows the cast for some reason
        "-inf" == f32::NEG_INFINITY, "toFloat32(-3.40282347e+38)" == f32::MIN,
        "-1.0" == -1.0, "0.0" == 0.0, "toFloat32(1.17549435e-38)" == f32::MIN_POSITIVE,
        "1.0" == 1.0, "toFloat32(3.14)" == 3.14,
        "toFloat32(3.40282347e+38)" == f32::MAX, "inf" == f32::INFINITY,
        // "nan" == f32::NAN, (NaN is never equal to NaN)
    }
);

test_type!(
    #[allow(clippy::approx_constant)] // we can't be certain the constant for pi is the same
    test_float64(f64, "Float64") {
        "-inf" == f64::NEG_INFINITY, "-1.7976931348623157e+308" == f64::MIN,
        "-1.0" == -1.0, "0.0" == 0.0, "2.2250738585072014e-308" == f64::MIN_POSITIVE,
        "1.0" == 1.0, "3.14" == 3.14,
        "1.7976931348623157e+308" == f64::MAX, "inf" == f64::INFINITY,
        // "nan" == f32::NAN, (NaN is never equal to NaN)
    }
);

test_type!(
    test_ipv4(Ipv4Addr, "IPv4") {
        "'0.0.0.0'" == Ipv4Addr::UNSPECIFIED,
        "'1.1.1.1'" == Ipv4Addr::new(1, 1, 1, 1),
        "'127.0.0.1'" == Ipv4Addr::LOCALHOST,
        "'192.168.2.1'" == Ipv4Addr::new(192, 168, 2, 1),
        "'255.255.255.0'" == Ipv4Addr::new(255, 255, 255, 0),
        "'255.255.255.255'" == Ipv4Addr::BROADCAST,
    }
);

test_type!(
    test_ipaddr_from_v4(IpAddr, "IPv4") {
        "'0.0.0.0'" == Ipv4Addr::UNSPECIFIED,
        "'1.1.1.1'" == Ipv4Addr::new(1, 1, 1, 1),
        "'127.0.0.1'" == Ipv4Addr::LOCALHOST,
        "'192.168.2.1'" == Ipv4Addr::new(192, 168, 2, 1),
        "'255.255.255.0'" == Ipv4Addr::new(255, 255, 255, 0),
        "'255.255.255.255'" == Ipv4Addr::BROADCAST,
    }
);

test_type!(
    test_ipv6(Ipv6Addr, "IPv6") {
        "'::1'" == Ipv6Addr::LOCALHOST,
        // IPv6 addresses for ClickHouse.com
        "'2606:4700:3108::ac42:2b07'" == "2606:4700:3108::ac42:2b07".parse::<Ipv6Addr>().unwrap(),
        "'2606:4700:3108::ac42:28f9'" == "2606:4700:3108::ac42:28f9".parse::<Ipv6Addr>().unwrap(),
    }
);

test_type!(
    test_ipaddr_from_v6(IpAddr, "IPv6") {
        "'::1'" == Ipv6Addr::LOCALHOST,
        // IPv6 addresses for ClickHouse.com
        "'2606:4700:3108::ac42:2b07'" == "2606:4700:3108::ac42:2b07".parse::<Ipv6Addr>().unwrap(),
        "'2606:4700:3108::ac42:28f9'" == "2606:4700:3108::ac42:28f9".parse::<Ipv6Addr>().unwrap(),
    }
);

#[cfg(feature = "uuid")]
mod uuid {
    use uuid::Uuid;

    test_type!(
        test_uuid(Uuid, "UUID") {
            "'00000000-0000-0000-0000-000000000000'" == Uuid::from_bytes([0u8; 16]),
            "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'" == "61f0c404-5cb3-11e7-907b-a6006ad3dba0".parse::<Uuid>().unwrap(),
            "'67e55044-10b1-426f-9247-bb680e5fe0c8'" == "67e55044-10b1-426f-9247-bb680e5fe0c8".parse::<Uuid>().unwrap(),
        }
    );
}
