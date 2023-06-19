package flinkdemo.util.visual;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ExcelWriter {
    public static void addTimeToExcel(String fileName, HashMap<String, Long> resultMap, int round) throws IOException {
        File file = new File(fileName);
        Workbook wb;
        Sheet sheet;
        if (file.exists()) {
            wb = WorkbookFactory.create(new FileInputStream(file));
            sheet = wb.getSheet("new sheet");
        } else {
            wb = new XSSFWorkbook();
            sheet = wb.createSheet("new sheet");
        }
        Row row = sheet.createRow(round);
        for(Map.Entry<String, Long> entry : resultMap.entrySet()) {
            row.createCell(Integer.parseInt(entry.getKey())).setCellValue((double) entry.getValue());
        }
        try (OutputStream fileOut = new FileOutputStream(fileName)) {
            wb.write(fileOut);
        }
    }

    public static void addMapSizeToExcel(String fileName, int landmark2, int landmark1, int landmark0) throws IOException {
        File file = new File(fileName);
        Workbook wb;
        Sheet sheet;
        if (file.exists()) {
            wb = WorkbookFactory.create(new FileInputStream(file));
            sheet = wb.getSheet("new sheet");
        } else {
            wb = new XSSFWorkbook();
            sheet = wb.createSheet("new sheet");
        }
        Row row = sheet.createRow(0);
        row.createCell(0).setCellValue(landmark2);
        row.createCell(1).setCellValue(landmark1);
        row.createCell(2).setCellValue(landmark0);
        try (OutputStream fileOut = new FileOutputStream(fileName)) {
            wb.write(fileOut);
        }
    }

    public static void addHitRatioToExcel(String fileName, int queryNumber, int hitNumber, int round) throws IOException {
        File file = new File(fileName);
        Workbook wb;
        Sheet sheet;
        if (file.exists()) {
            wb = WorkbookFactory.create(new FileInputStream(file));
            sheet = wb.getSheet("new sheet");
        } else {
            wb = new XSSFWorkbook();
            sheet = wb.createSheet("new sheet");
        }
        Row row = sheet.createRow(round);
        row.createCell(0).setCellValue( (double)hitNumber / queryNumber);
        try (OutputStream fileOut = new FileOutputStream(fileName)) {
            wb.write(fileOut);
        }
    }
}
