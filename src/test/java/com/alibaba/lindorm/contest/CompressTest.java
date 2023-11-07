package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.test.RandomUtils;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.Deflater;

public class CompressTest {
    public static void main(String[] args) {
//        String str = "SUCCESS";
//        for(int i = 0;i < 10;++i){
//            str = str + str;
//        }
//        String str = "AA=k]2!McAo;!g]UAc!cU(QgcQEEAU;g6M==%AwQ;/,2sUU2Y62,;]2I,A,c]gwMkYoQwsUYUs2==MYQosI](E/U=gkY%A22EcIY/,,,/s==Q!g(,E!/I6MwkwsI;A2IAg%M!Uw%YwE!UM2M6oQ((k!Y;g/2gk%kgoYooIQAEc(ssU,k(6!sYM%w(s;=w2cs!oMkkA/(QEU6Y;YQ,kEME%wsg=AwY!Y22U2!6M]sM=w,ocA=wIw//2;Q2s=wkMEI,okgYI(2=6Mw]!(((]%kQk(!6/Qw%YUcM=YcIwMAAA=,cU(=gkswg2,M%wA/%c,=cos!MsEEQc;QccIQQ]kI%k=IgwQ6Y6/2o/oYQ/,w]w]cYA,]AUI]IEMs;!2wI(IUkIwg2c(6/QsMY%6s,o]g622A6cMos6]2Msk!E/gE%6sg%%(woA,,U%c%(Q2;EY%=As(Ywwc!s!ggw=skUkgs2E]EY]c%]Qwo(;66]26MM6M/=Y22;wQ=(EEQ%;!6MAQU=!U=(6/,wEk6AE6(MwA;2IgcYM]k;;]=A]2AYQ6Y/YAcM/!,cswMM2E!6kkM/;2U%Y%M,]%wk(;wkAwAs,UkM(;II!,2;%cks!gQ(I,EowsQ6;I(MM=;UE%Yo6AgM2wQA62Qc!/MYUEA;IUsU2gEEYUsU%AU,!]]Qw,=UMkw;I;6o;6gQwUkk/Mw,Q6kokA%w]!Ew6oo,,w=2,6%(EU%o,Igg%=,6QkQ2kso%Q%6MkIs=(2kU!2wgcMYQI;M;Q!E(cc%g6Y]M=MI2EcE/,62csk/2U22IM2woQ,k6,(s=YIQ2=//(s!g2cQ//2=c;A(2gIE/(o;!UMw;E/Y(k/I%Y2k!A2YIE;;%6c]YU;;!!gcY%AgEA2wYo=c%6s/;I;gc%YUk!66Y]/6QwQ(2=kkA622gc%,,AIM6,Ek=!%]kE,/EAQ;=ssgskMYgEkcM=QQw(sk;=c]AQw(2Y;IAo(2EcYoMQIsA]ow]gY;Uw;g(/;,sk2EY(k!6Y]EkI2,EUUE!wos,/IEocM;6gc%AE]A6AAsUIQ2gM]MIU,kYAIs(MsEk]EoUIYc(c/woIsgA=kQg/,Q;UUwQUA!E(AM!;!6!E/wM,ck(c!U2wMU/!EocA,]EwAwsI!%/,]Mk=,6]%QoM(;gME]UA]2(;!MM]kkA6M/!=QQ%!6cgcA;U=Q(Qo2s6IMkE]=AcAsc]%M6g;Qk/s,/U%s2AUUk;YM!(/k2=o=Q2(cEE/UA(6Yc6oo%Q%Y]E/,=A(cYUMo%E]osko(=QY(Q;,/sw(k%=o(AgE%=/c266Y=QM!,w(w2Uc;/cA!Ukw;%(s,!,6;%Yg2!!AkIUwoQEk/ko;Qo%/sYI%A(gck!=w,;,A(MAAc;AIQ%A6Awg!cg(w!Ew2MUYE/IUoUEo%M/Q,IY;(YM6;=QIUYA66M!(!gMUYcM]!A,(s=%6]As(EE(s%]6oU=M;]E!]22wU,kc,2=cUQAU%,]oQ2,]/cs,,QAU,Ag=AUY2coYQAAg,AUE];s=];c]oIco6Q(AM;,AQ2;=wgY=,=!o;goQ%2MUgEUYwAYQUs,A,AcY2U]UEMYcAsgQ%s!!(c!]MUc,%2;6k%/w]6!=2/EAIo!,g%AU,%!EMAI6A!(!kc26Mk6c!;U=s,6%!YM,6(%;k!U/Mo,6o6%2A2Ugc!s2!cwo!g(,M%/%=s=QQII/Eo;!s;occ]2o!2s=Y2gQAgkU,MUsgUUs/(wsgM,c]2,(MEso2IA6=MU2k/6=ck6;U/6AMMY;%o%kU]EsI;kk=,%s;;ks(E%!%IA2!oE!wM(%woQ(6Y!YwQ/(k%],Akg]%k=Y6Y;gY2(;UcI(soo!U=]2QU=%oAM=AQ!]Y%Mwo6o(AIAw=Eks62AMA((sQoMY22YYc%w]A2;k!=wco==cYUkE6YgQQEkYoowE/6!c%](%/s=/w;/c6s;I/g2w,/2=(=;g(=wY/U];,UkMQY%YQQws]oQI,IQ;EE%Mw!6/,];o/M=6]Q%;%w2]%!QswoEkwoog,c/Q,EYMw6Ykw/g;o;c]%/oU2E%AkUwoQEo/EYogY6QgckkIUwMMY((";
//        byte[] input = str.getBytes();
//        byte[] output = new byte[10000];
//        Deflater compresser = new Deflater();
//        compresser.setInput(input);
//        compresser.finish();
//        int len0 = str.length();
//        int len1 = compresser.deflate(output);
//        System.out.printf("original length:%d, compressed length:%d, ratio:%f\n",len0,len1,(double)len1/len0);
        millisTest();
    }
    public static void IntegerTest(){
        ByteBuffer buffer = ByteBuffer.allocate(3600*4);
        Random random = new Random();
        for(int i = 0;i < 3600;++i){
            buffer.putInt(1);
        }
        buffer.position(0);
        Deflater compresser = new Deflater();
        compresser.setInput(buffer);
        compresser.finish();
        ByteBuffer outputBuffer = ByteBuffer.allocate(3600*6);
        int len0 = 3600*4;
        int len1 = compresser.deflate(outputBuffer);
        System.out.printf("original length:%d, compressed length:%d, ratio:%f\n",len0,len1,(double)len1/len0);
    }
    public static void doubleTest(){
        ByteBuffer mBuffer = ByteBuffer.allocate(3600*Long.BYTES);
        ByteBuffer eBuffer = ByteBuffer.allocate(3600*Short.BYTES);
        Random random = new Random();
        for(int i = 0;i < 3600;++i){
            double d = (random.nextInt(10000)-5000)/10000.0;
            long longbits = Double.doubleToRawLongBits(d);
            long mantissa = longbits & 0xf_ff_ff_ff_ff_ff_ffL;
            short exponent = (short)((longbits >>> 52) & 0xfff);
            mBuffer.putLong(mantissa);
            eBuffer.putShort(exponent);
        }
        mBuffer.position(0);
        eBuffer.position(0);
        Deflater compresser = new Deflater();
        compresser.setInput(mBuffer);
        compresser.finish();
        Deflater compresser0 = new Deflater();
        compresser0.setInput(eBuffer);
        compresser0.finish();
        ByteBuffer outputBuffer = ByteBuffer.allocate(3600*(Long.BYTES+2));
        ByteBuffer outputBuffer0 = ByteBuffer.allocate(3600*(Long.BYTES+2));
        int len0 = 3600*8;
        int len1 = compresser.deflate(outputBuffer)+compresser0.deflate(outputBuffer0);
        System.out.printf("original length:%d, compressed length:%d, ratio:%f\n",len0,len1,(double)len1/len0);
    }
    public static void rawDoubleTest(){
        ByteBuffer buffer = ByteBuffer.allocate(3600*Double.BYTES);
        Random random = new Random();
        for(int i = 0;i < 3600;++i){
            double d = 0.01;
            buffer.putDouble(d);
        }
        buffer.position(0);
        Deflater compresser = new Deflater();
        compresser.setInput(buffer);
        compresser.finish();
        ByteBuffer outputBuffer = ByteBuffer.allocate(3600*(Double.BYTES+2));
        int len0 = 3600*Double.BYTES;
        int len1 = compresser.deflate(outputBuffer);
        System.out.printf("original length:%d, compressed length:%d, ratio:%f\n",len0,len1,(double)len1/len0);
    }

    public static void millisTest(){
        ByteBuffer buffer = ByteBuffer.allocate(3600*Short.BYTES);
        Random random = new Random();
        for(int i = 0;i < 3600;++i){
            buffer.putShort((short)random.nextInt(1000));
        }
        buffer.position(0);
        Deflater compresser = new Deflater();
        compresser.setInput(buffer);
        compresser.finish();
        ByteBuffer outputBuffer = ByteBuffer.allocate(3600*(Double.BYTES+2));
        int len0 = 3600*Short.BYTES;
        int len1 = compresser.deflate(outputBuffer);
        System.out.printf("original length:%d, compressed length:%d, ratio:%f\n",len0,len1,(double)len1/len0);
    }
}
