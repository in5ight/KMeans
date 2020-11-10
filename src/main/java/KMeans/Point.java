package KMeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Point implements Writable {
    private double x;
    private double y;
    ArrayList<Double> value;

    public Point() {
        x = 0.0;
        y = 0.0;
    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point(String line) {
        String[] valueString = line.split(",");
        x = Double.parseDouble(valueString[0]);
        y = Double.parseDouble(valueString[1]);
    }

    public Point(Point pnt) {
        x = pnt.getx();
        y = pnt.gety();
    }

    public Point(int k) {
        x = 0;
        y = 0;
    }

    public Double getx() {
        return x;
    }

    public Double gety() {
        return y;
    }

    public Point add(Point pnt) {
        Point result = new Point(x + pnt.getx(), y + pnt.gety());
        return result;
    }

    public Point multiply(double num) {
        Point result = new Point(x * num, y * num);
        return result;
    }

    public Point divide(double num) {
        Point result = new Point(x / num, y / num);
        return result;
    }

    public String toString() {
        String s = new String();
        s += (x + "," + y);
        return s;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
    }
}