package com.alibaba.middleware.race.sync;

/**
 * 外部赛示例代码需要的常量 Created by wanshao on 2017/5/25.
 */
public interface Constants {

    String[] TEST_ROLE = {"server", "client"};
    // teamCode
    String TEAMCODE = "78921g1clv";
    // 日志级别
    String LOG_LEVEL = "INFO";
    // server端口
    Integer SERVER_PORT = 5527;
    // 结果文件的命名
    String RESULT_FILE_NAME = "Result.rs";

    //Client threads number
    Integer CLIENT_THREADS_NUMBER = 24;

    // ------------ 本地测试可以使用自己的路径--------------//
    //%%%%%%%%%%%%%%%%% change bellow %%%%%%%%%%%%%%%%%
    // 工作主目录
    String TESTER_HOME = "/tmp/test";
    // 赛题数据
    String DATA_HOME = "/tmp/test/canal_data";
    // 结果文件目录
    String RESULT_HOME = "/tmp/test/user_result";
    // 中间结果目录
    String MIDDLE_HOME = "/tmp/test/middle";
    //%%%%%%%%%%%%%%%%% change above %%%%%%%%%%%%%%%%%


    // ------------ 正式比赛指定的路径--------------//
    //%%%%%%%%%%%%%%%%% change bellow %%%%%%%%%%%%%%%%%
    //// 工作主目录
    //String TESTER_HOME = "/home/admin";
    //// 赛题数据
    //String DATA_HOME = "/home/admin/canal_data";
    //// 结果文件目录(client端会用到)
    //String RESULT_HOME = "/home/admin/sync_results/"+TEAMCODE;
    //// 中间结果目录（client和server都会用到）
    //String MIDDLE_HOME = "/home/admin/middle/" + TEAMCODE;
    //%%%%%%%%%%%%%%%%% change above %%%%%%%%%%%%%%%%%

    char SPLIT_CHAR = '|';
    char FIELD_SPLIT_CHAR = ':';
    char IS_PRIMARY_KEY = '1';
    char IS_NUMBER = '1';

    char INSERT_OPERATION = 'I';
    char DELETE_OPERATION = 'D';
    char UPDATE_OPERATION = 'U';

    final static int CHUNK_SIZE = 8 * 1024;

}
