package io.openmessaging;

/**
 * @author pengqun.pq
 */
class Constants {

    static final String DATA_DIR = "/alidata1/race2019/data/"; static final int TEST_BOUNDARY = 37000; static final int PRODUCER_THREAD_NUM = 12;
//    static final String DATA_DIR = "/tmp/"; static final int TEST_BOUNDARY = 9000; static final int PRODUCER_THREAD_NUM = 10;

    static final int KEY_A_BYTE_LENGTH = 8;
    static final int BODY_BYTE_LENGTH = 34;
    static final int STAGE_MSG_BYTE_LENGTH = 43;
//    static final int STAGE_MSG_BYTE_LENGTH = 42;

    static final int T_INDEX_SUMMARY_FACTOR = 64;

    static final int A_INDEX_MAIN_BLOCK_SIZE = 1024 * 12;
    static final int A_INDEX_SUB_BLOCK_SIZE = 1024 * 3;
    static final int A_INDEX_META_FACTOR = 32;

    static final int WRITE_STAGE_BUFFER_SIZE = STAGE_MSG_BYTE_LENGTH * 1024;
    static final int READ_STAGE_BUFFER_SIZE = STAGE_MSG_BYTE_LENGTH * 1024 * 16;

    static final int WRITE_A_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024;
    static final int READ_A1_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 8;
    static final int READ_A2_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 16;

    static final int WRITE_AI_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024;
//    static final int READ_AIM_BUFFER_SIZE = KEY_A_BYTE_LENGTH * A_INDEX_META_FACTOR;
    static final int READ_AIM_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 16;
    static final int READ_AIS_BUFFER_SIZE = KEY_A_BYTE_LENGTH * 1024 * 16;

    static final int WRITE_BODY_BUFFER_SIZE = BODY_BYTE_LENGTH * 1024;
    static final int READ_BODY_BUFFER_SIZE = BODY_BYTE_LENGTH * 1024;
}
