package pr.studb;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.*;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ParquetWriteAndRead {

    @Test
    void writeParquetInFileSystem() throws IOException {
        Schema schema = SchemaBuilder
                .record("record")
                .namespace("namespace")
                .fields()
                .name("col1").type().nullable().stringType().noDefault()
                .name("col2").type().nullable().stringType().noDefault()
                .name("col3").type().nullable().stringType().noDefault()
                .endRecord();

        Path path = new org.apache.hadoop.fs.Path("/tmp/file.parquet");
        OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(outputFile)
                .withSchema(schema)
                .build();

        GenericData.Record record = new GenericData.Record(schema);
        record.put("col1", "Col1 Data1"); record.put("col2", "Col2 Data1"); record.put("col3", "Col3 Data1");
        writer.write(record);
        record.put("col1", "Col1 Data2"); record.put("col2", "Col2 Data2"); record.put("col3", "Col3 Data2");
        writer.write(record);

        writer.close();
    }

    @Test
    void readParquetFromFileSystem() throws IOException {
        Path path = new org.apache.hadoop.fs.Path("/tmp/file.parquet");
        InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
        ParquetFileReader reader = ParquetFileReader.open(inputFile);

        System.out.println("> Metadata Print");
        ParquetMetadata metadata = reader.getFooter();
        System.out.println(metadata);

        System.out.println("> Data Print");
        MessageType messageType = reader
                .getFooter()
                .getFileMetaData()
                .getSchema();
        PageReadStore pageReadStore = null;
        while ( (pageReadStore = reader.readNextFilteredRowGroup()) != null ) {
            final long rows = pageReadStore.getRowCount();
            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(messageType);
            final RecordReader recordReader = columnIO.getRecordReader(pageReadStore, new GroupRecordConverter(messageType));
            for (int i = 0; i < rows; i++) {
                System.out.println(String.format(">> %d 번째 Record", i+1));
                System.out.print(recordReader.read());
            }
        }

        reader.close();
    }
}
