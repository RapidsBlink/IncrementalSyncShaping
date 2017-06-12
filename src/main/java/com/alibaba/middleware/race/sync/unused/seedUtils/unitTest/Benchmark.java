package com.alibaba.middleware.race.sync.unused.seedUtils.unitTest;

import com.alibaba.middleware.race.sync.unused.seedUtils.Fastu;

import java.io.UnsupportedEncodingException;
import java.util.Calendar;

public class Benchmark {
    private static final int TOTAL_OPERATIONS = 100000;

    public static void main(String[] args) throws UnsupportedEncodingException {
        byte[] aboveAscii = "因为无论你如何选择 年代出生的人都会书写常用的繁体汉字事实上 平话 了, 格 相反 为基础的书面语 在现代汉语的书面语中, 徽语 性 比较简单些 后者用于中国大陆和新加坡以及东南亚的华人社区 闽语 余皆不论 而在台湾和香港使用 大陆也直客观存在简繁并用的现象其二 找, 格 简化汉字直颇受争议 公元前世纪到世纪 国标, 华语 如上面提到的 只使用末尾的语气助词 头 格 徽语 总有些字是使用错误的 简体转换为繁体通常需要根据内容和用法来决定使用的字词, 四川 及 的码 以及些表示不同动作状态的助词 现在简化汉字主要通行于中国大陆, 在这七大方言内部 些学校也已经把课本转化成了简体中文版本 简体转换为繁体通常需要根据内容和用法来决定使用的字词 性 然而 以后 老 文言已经很少使用了 自推行汉语文字改革以来 这些资料仍然是现代语言学家工作的基础, 共有 高本汉把这个阶段称为 常用于表示已经发生的动作 体, 和 助词也用来表达问句问句的语序与陈述句的语序相同 助词等所构成的句法复杂程度却又大大地超过了以拉丁语为例的曲折性语言 最后 韩语 还是繁简并用 上古汉语存在于周朝前期和中期 但是两者之间在编码上和字体上没有同的标准 体 成语 走上来 汉语中存在 特别是形声字 体 走 推广普通话的效率 以及些表示不同动作状态的助词 贵州, 台湾 汉语中存在 广西东南部等地 以福州话为代表 了 菲律宾和南亚的些国家使用 中文的词汇只有种形式而没有诸如复数 大陆也直客观存在简繁并用的现象其二 文言 新 和 相反 湖南北部 繁体有些书写形式更有美感 汉语在广义上是指汉民族的语言, 大陆直至年代 只使用末尾的语气助词 第个是与目前发生的事相关的 吗 平话, 打 海南 汉语属于独立语 在现代汉语的书面语中 通常被分为老和新两类 湖南东南部 例如在普通话中的 找 最后 走 为基础的书面语 最新的版本是 然而, 打 广西东南部等地 有人认为这是白话文与古文相异 实际上已经形成了现代北方话的雏形 闽语 相反 请参看汉字 在地理上的方言分歧也是很明显的 正如日耳曼语-印欧语系的语言可以由现代印欧语言重构样 新 我皆不论 五四运动之后所推动的书面汉语通常被称为 走 找到, 但是编码和字体又通常因为实际原因而联系在起 繁体中文的使用似乎不大可能回复到以前的统治地位 和 徽语 粤方言是汉语中声调最复杂的方言之 闽语是所有方言中唯不与中古汉语韵书存在直接对应的方言 上 然而, 走 对多 使用客家话的人口大约占总人口的 贵州 成语 新 在江苏南部 时态等的曲折变化, 找到 走 赣方言 内蒙古河套地区等地使用 常用汉字的个体差异不到"
                .getBytes("UTF-8");
        byte[] ascii = "Lorem ipsum ullum persequeris cum ei. Mel ludus tation accusamus eu, aeterno forensibus pri id, ius no adipisci quaestio. Id dico kasd quando per, graecis apeirian eu mel, qui eu magna perpetua consulatu. At quodsi maiestatis assueverit vis, sed ea quod molestie."
                .getBytes("UTF-8");

        benchmark(ascii, "Only ASCII chars");
        benchmark(aboveAscii, "Other UNICODE chars");

    }

    private static void benchmark(byte[] bytes, String desc)
            throws UnsupportedEncodingException {
        System.out.println("");
        System.out.println(desc);
        System.out.println("******************************************");
        long begin = Calendar.getInstance().getTimeInMillis();

        for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
            new String(bytes, "UTF-8");
        }

        long elapsed = Calendar.getInstance().getTimeInMillis() - begin;
        long opsNative = (1000 * TOTAL_OPERATIONS) / elapsed;
        System.out.println(opsNative + " ops with native java.");

        begin = Calendar.getInstance().getTimeInMillis();

        for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
            Fastu.decode(bytes);
        }

        elapsed = Calendar.getInstance().getTimeInMillis() - begin;
        long opsFastu = (1000 * TOTAL_OPERATIONS) / elapsed;
        System.out.println(opsFastu + " ops with Fastu.");

        System.out.println((double) opsFastu / (double) opsNative);
    }
}