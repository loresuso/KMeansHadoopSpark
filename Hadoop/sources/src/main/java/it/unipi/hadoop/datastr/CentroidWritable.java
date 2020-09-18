package it.unipi.hadoop.datastr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class CentroidWritable extends PointWritable {
    
    protected int weight;

    public CentroidWritable() {
        super();
    }
    
    public CentroidWritable(PointWritable point) {
        this(point.getCoords(),point.getDim(),1);
    }
    
    public CentroidWritable(float[] coords, int dim) {
        this(coords, dim, 1);
    }
    
    public CentroidWritable(float[] coords, int dim, int weight) {
        super(coords, dim);
        this.weight = weight;
    }
    
    public CentroidWritable(String str) {
        super(str);
        this.weight = 1;
    }

    public void resetToZero() {
        for(int i = 0; i < dim; ++i)
            coords[i] = 0;
        weight = 1;
    }
    
    public void linkTo(PointWritable point) {
        coords = point.getCoords();
        dim = point.getDim();
        weight = 1;
    }
    
    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
    
    @Override
    public int compareTo(Object o) {
        if(!(o instanceof CentroidWritable)) {
            return -1;
        }
        CentroidWritable other = (CentroidWritable)o;
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
        if(weight < other.weight) {
            return -1;
        }
        if(weight > other.weight) {
            return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + Arrays.hashCode(this.coords);
        hash = 43 * hash + this.dim;
        //hash = 43 * hash + this.weight;
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
        final CentroidWritable other = (CentroidWritable) obj;
        if(this.dim != other.dim) {
            return false;
        }
        if(!Arrays.equals(this.coords, other.coords)) {
            return false;
        }
        /*if(this.weight != other.weight) {
            return false;
        }*/
        return true;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dim);
        for(int i = 0; i < dim; i++){
            out.writeFloat(coords[i]);
        }
        out.writeInt(weight);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        dim = in.readInt();
        coords = new float[dim];
        for(int i = 0; i < dim; i++){
            coords[i] = in.readFloat();
        }
        weight = in.readInt();
    }
    
}
