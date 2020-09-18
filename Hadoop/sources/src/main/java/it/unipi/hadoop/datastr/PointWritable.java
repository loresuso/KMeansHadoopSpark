package it.unipi.hadoop.datastr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 */
public class PointWritable implements WritableComparable {
    
    protected float[] coords;
    protected int dim;

    public PointWritable() {
        
    }
    
    public PointWritable(float[] coords, int dim) {
        this.coords = coords;
        this.dim = dim;
    }
    
    public PointWritable(String str) {
        String[] splitted = str.split(",");
        dim = splitted.length;
        coords = new float[dim];
        for(int i = 0; i < dim; ++i)
            coords[i] = Float.parseFloat(splitted[i]);
    }

    public void updateFromLine(String line) {
        String[] splitted = line.split(",");
        dim = splitted.length;
        for(int i = 0; i < dim; ++i)
            coords[i] = Float.parseFloat(splitted[i]);
    }
    
    public float[] getCoords() {
        return coords;
    }

    public void setCoords(float[] coords) {
        this.coords = coords;
    }

    public int getDim() {
        return dim;
    }

    public void setDim(int dim) {
        this.dim = dim;
    }
    
    public void sum(PointWritable other) {
        if(dim != other.dim) {
            throw new RuntimeException("Different dim!");
        }
        for(int i = 0; i < dim; ++i) {
            coords[i] += other.coords[i];
        }
    }
    
    public float distanceTo(PointWritable other) {
        if(dim != other.dim)
            throw new RuntimeException("distanceTo(...) called with different dims");
        float distance = 0.0f;
        float term;
        for(int i = 0; i < dim; ++i) {
            term = coords[i] - other.coords[i];
            distance += term * term;
        }
        return distance;
    }
    
    public void divideCoordsBy(float divisor) {
        for(int i = 0; i < coords.length; ++i) {
            coords[i] /= divisor;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < coords.length; ++i) {
            sb.append(Float.toString(coords[i]));
            if(i < coords.length-1)
                sb.append(",");
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + Arrays.hashCode(this.coords);
        hash = 43 * hash + this.dim;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PointWritable other = (PointWritable) obj;
        if (this.dim != other.dim) {
            return false;
        }
        if (!Arrays.equals(this.coords, other.coords)) {
            return false;
        }
        return true;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dim);
        for(int i = 0; i < dim; i++){
            out.writeFloat(coords[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dim = in.readInt();
        coords = new float[dim];
        for(int i = 0; i < dim; i++){
            coords[i] = in.readFloat();
        }
    }

    @Override
    public int compareTo(Object o) {
        if(!(o instanceof PointWritable)) {
            return -1;
        }
        PointWritable other = (PointWritable)o;
        if(dim <= 0) {
            return -1;
        }
        if(dim < other.dim) {
            return -1;
        }
        if(dim > other.dim) {
            return 1;
        }
        for(int i = 0; i < dim; ++i) {
            if(coords[i] < other.coords[i])
                return -1;
            if(coords[i] > other.coords[i])
                return 1;
        }
        return 0;
    }
    
}
