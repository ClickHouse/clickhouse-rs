use std::fmt;

#[derive(Debug, Clone)]
pub enum OutputFormat {
    TabSeparated,
    TabSeparatedRaw,
    TabSeparatedWithNames,
    TabSeparatedWithNamesAndTypes,
    TabSeparatedRawWithNames,
    TabSeparatedRawWithNamesAndTypes,
    Template,
    CSV,
    CSVWithNames,
    CSVWithNamesAndTypes,
    CustomSeparated,
    CustomSeparatedWithNames,
    CustomSeparatedWithNamesAndTypes,
    SQLInsert,
    Values,
    Vertical,
    JSON,
    JSONStrings,
    JSONColumns,
    JSONColumnsWithMetadata,
    JSONCompact,
    JSONCompactStrings,
    JSONCompactColumns,
    JSONEachRow,
    PrettyJSONEachRow,
    JSONEachRowWithProgress,
    JSONStringsEachRow,
    JSONStringsEachRowWithProgress,
    JSONCompactEachRow,
    JSONCompactEachRowWithNames,
    JSONCompactEachRowWithNamesAndTypes,
    JSONCompactStringsEachRow,
    JSONCompactStringsEachRowWithNames,
    JSONCompactStringsEachRowWithNamesAndTypes,
    JSONObjectEachRow,
    BSONEachRow,
    TSKV,
    Pretty,
    PrettyNoEscapes,
    PrettyMonoBlock,
    PrettyNoEscapesMonoBlock,
    PrettyCompact,
    PrettyCompactNoEscapes,
    PrettyCompactMonoBlock,
    PrettyCompactNoEscapesMonoBlock,
    PrettySpace,
    PrettySpaceNoEscapes,
    PrettySpaceMonoBlock,
    PrettySpaceNoEscapesMonoBlock,
    Prometheus,
    Protobuf,
    ProtobufSingle,
    ProtobufList,
    Avro,
    Parquet,
    Arrow,
    ArrowStream,
    ORC,
    Npy,
    RowBinary,
    RowBinaryWithNames,
    RowBinaryWithNamesAndTypes,
    Native,
    Null,
    XML,
    CapnProto,
    LineAsString,
    RawBLOB,
    MsgPack,
    Markdown,
}

impl fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
