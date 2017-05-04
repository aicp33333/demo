/**
 * 
 */
package com.rpdemo.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author rongpei
 *
 */
public class CombineValues  implements WritableComparable<CombineValues> {
	
	private Text joinKey;//链接关键字   
    private Text flag;//文件来源标志   
   // private Text secondPart;//除了链接键外的其他部分   
    public void setJoinKey(Text joinKey) {   
        this.joinKey = joinKey;   
    }   
    public void setFlag(Text flag) {   
        this.flag = flag;   
    }   
     
    public Text getFlag() {   
        return flag;   
    }   
  
    public Text getJoinKey() {   
        return joinKey;   
    }   
    public CombineValues() {   
        this.joinKey =  new Text();   
        this.flag = new Text();   
         
    }
 
    public void write(DataOutput out) throws IOException {   
        this.joinKey.write(out);   
        this.flag.write(out);   
  
    }   
    
    public void readFields(DataInput in) throws IOException {   
        this.joinKey.readFields(in);   
        this.flag.readFields(in);   
       
    }   
   
    public int compareTo(CombineValues o) {
		int cmp = joinKey.compareTo(o.joinKey);
		if(cmp != 0){
			// 正序
			return cmp;
		}
		// 正序
		return flag.compareTo(o.flag);
	}
    @Override 
    public String toString() {   
        // TODO Auto-generated method stub   
        return "[flag="+this.flag.toString()+",joinKey="+this.joinKey.toString()+"]";   
    }   

    public static class FirstComparator extends WritableComparator{

		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		protected FirstComparator(Class<? extends WritableComparable> keyClass) {
			super(keyClass);
		}
		public FirstComparator(){
			super(CombineValues.class);
		}
	
		public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){
			
				int firstL1 = 0;
				int firstL2 = 0;
				try {
					firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
					firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);
				} catch (IOException e) {
					e.printStackTrace();
				}	
				
				return TEXT_COMPARATOR.compare(b1 , s1, firstL1 , b2 , s2 , firstL2);		
		}
		
		public int compare(WritableComparable a, WritableComparable b){
			if(a instanceof CombineValues && b instanceof CombineValues){
				return ((CombineValues)a).flag.compareTo(((CombineValues)b).flag);
			}
			return super.compare(a, b);
		}
		
	}

}
