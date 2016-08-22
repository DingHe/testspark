package com.zzx.sample;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import javax.swing.ImageIcon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
public class TestHadoopImage {
    public void hadooopreadpic() throws IOException {
        Configuration myConf = new Configuration();
        //设置你的NameNode地址
        myConf.set("fs.default.name", "hdfs://m01.bj.tcb.hdp:9000");
        //图片存放在HDFS上的路径
        String hdfspath = "hdfs://m01.bj.tcb.hdp:9000/tmp/1.png";
        FileSystem myFS = FileSystem.get(myConf);
        FSDataInputStream in = myFS.open(new Path(URI.create(hdfspath)));
        //根据图片大小设置Buffer，大点也没关系
        byte[] Buffer = new byte[1024 * 1024];
        in.read(Buffer);
        ImageIcon[] image = { new ImageIcon(Buffer) };
        BufferedImage img = new BufferedImage((image.length) * 256, 256, BufferedImage.TYPE_INT_RGB);
        Graphics2D gs = (Graphics2D) img.getGraphics();
        for (int i = 0; i < image.length; i++) {
            String k = "";
        }
        gs.drawImage(image[0].getImage(), 0, 0, image[0].getImageObserver());
        int huabuwid = img.getWidth();
        int huabuhid = img.getHeight();
        for (int i = 0; i < huabuwid; i++) {
            for (int j = 0; j < huabuhid; j++) {
                // 基于坐标取出相对应的RGB
                int rgb = img.getRGB(i, j);
 
                int R = (rgb & 0xff0000) >> 16;
                int G = (rgb & 0xff00) >> 8;
                int B = (rgb & 0xff);
                //打印图片坐标和RGB三元色
                System.out.println(String.format("i:%d j:%d R:%d G:%d B:%d", i, j, R, G, B));
            }
        }
        // 释放Graphics2D对象
        gs.dispose();
    }
 
    public static void main(String[] args) throws IOException {
        new TestHadoopImage().hadooopreadpic();
    }
}