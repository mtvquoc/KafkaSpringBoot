package com.example.KafkaSpringBoot.controller;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.nio.charset.StandardCharsets;

@RestController
public class IndexController implements ErrorController {
    private static final String ErrorPath = "/error";

    @RequestMapping(value = "")
    public String index() {
        return "Hello Spring Boot";
    }
    @RequestMapping(value = ErrorPath)
    public String error() {
        return "Error Handling";
    }

    @RequestMapping(value = "validate")
    public String validate() {
        try {
            var schemaStr = new ClassPathResource("static/test.avsc").getContentAsString(StandardCharsets.UTF_8);
            var json = new ClassPathResource("static/a.json").getContentAsString(StandardCharsets.UTF_8);
            var schema = new Schema.Parser().parse(schemaStr);
            var error = validateJson(json, schema);
            return error;
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private static String validateJson(String json, Schema schema) throws Exception {
        try {
            InputStream input = new ByteArrayInputStream(json.getBytes());
            DataInputStream din = new DataInputStream(input);
            DatumReader reader = new GenericDatumReader(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            reader.read(null, decoder);
            return "success";
        }catch (AvroTypeException ex) {
            return ex.getMessage();
        }

    }
}
