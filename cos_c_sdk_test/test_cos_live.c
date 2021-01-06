#include "cos_http_io.h"
#include "cos_api.h"
#include "cos_log.h"
#include <stdint.h>
#include <pthread.h>
#include <sys/stat.h>

static char TEST_COS_ENDPOINT[] = "cos.ap-guangzhou.myqcloud.com";
static char TEST_ACCESS_KEY_ID[] = "xxx";   //your secret_id
static char TEST_ACCESS_KEY_SECRET[] = "xxx";   //your secret_key
static char TEST_APPID[] = "xxx";    //your appid
static char TEST_BUCKET_NAME[] = "xxx";    //the cos bucket name, syntax: [bucket]-[appid], for example: mybucket-1253666666
static char TEST_REGION[] = "ap-guangzhou";    //region in endpoint
static char TEST_HOST_IP[] = "xxx";    //using host ip

void init_test_config(cos_config_t *config, int is_cname)
{
    cos_str_set(&config->endpoint, TEST_COS_ENDPOINT);
    cos_str_set(&config->access_key_id, TEST_ACCESS_KEY_ID);
    cos_str_set(&config->access_key_secret, TEST_ACCESS_KEY_SECRET);
    cos_str_set(&config->appid, TEST_APPID);
    config->is_cname = is_cname;
}

void init_test_request_options(cos_request_options_t *options, int is_cname)
{
    options->config = cos_config_create(options->pool);
    init_test_config(options->config, is_cname);
    options->ctl = cos_http_controller_create(options->pool, 0);
}

void log_status(cos_status_t *s)
{
    cos_warn_log("status->code: %d", s->code);
    if (s->error_code) cos_warn_log("status->error_code: %s", s->error_code);
    if (s->error_msg) cos_warn_log("status->error_msg: %s", s->error_msg);
    if (s->req_id) cos_warn_log("status->req_id: %s", s->req_id);
}


void test_live_channel()
{
    cos_pool_t *pool = NULL;
    int is_cname = 0;
    cos_status_t *status = NULL;
    cos_request_options_t *options = NULL;
    cos_table_t *resp_headers = NULL;
    cos_string_t bucket;
    char *content = NULL;
    cos_pool_create(&pool, NULL);

    options = cos_request_options_create(pool);
    options->config = cos_config_create(options->pool);
    init_test_request_options(options, is_cname);
    if (strlen(TEST_HOST_IP) > 0) {
        options->ctl->options->host_ip = TEST_HOST_IP;
        options->ctl->options->host_port = 80;
    }
    cos_str_set(&bucket, TEST_BUCKET_NAME);

    // create channel
    cos_list_t publish_url_list;
    cos_list_t play_url_list;
    cos_live_channel_configuration_t *chan_config = 
        cos_create_live_channel_configuration_content(options->pool);
    cos_str_set(&chan_config->name, "ch1");
    cos_str_set(&chan_config->description, "test live channel");

    cos_list_init(&publish_url_list);
    cos_list_init(&play_url_list);

    printf("create live channel=====================\n");
    status = cos_create_live_channel(options, &bucket, chan_config, &publish_url_list,
        &play_url_list, NULL);
    if (status->code != 200) {
        printf("failed to create live channel\n");
        log_status(status);
    }

    // get info
    cos_string_t chan_name;
    cos_live_channel_configuration_t chan_info;
    cos_str_set(&chan_name, "ch1");
    printf("get live channel info=====================\n");
    status = cos_get_live_channel_info(options, &bucket, &chan_name, &chan_info, NULL);
    if (status->code != 200) {
        printf("failed to get live channel info\n");
        log_status(status);
    } else {
        content = apr_psprintf(pool, "%.*s", chan_info.name.len, chan_info.name.data);
        printf("name:%s\n", content);
        content = apr_psprintf(pool, "%.*s", chan_info.description.len, chan_info.description.data);
        printf("description:%s\n", content);
        content = apr_psprintf(pool, "%.*s", chan_info.chan_switch.len, chan_info.chan_switch.data);
        printf("chan_switch:%s\n", content);
        content = apr_psprintf(pool, "%.*s", chan_info.target.type.len, chan_info.target.type.data);
        printf("type:%s\n", content);
        content = apr_psprintf(pool, "%.*s", chan_info.target.play_list_name.len, chan_info.target.play_list_name.data);
        printf("play_list_name:%s\n", content);
        printf("frag_duration:%d, frag_count:%d\n", chan_info.target.frag_duration, chan_info.target.frag_count);
    }

    // get status
    cos_live_channel_stat_t chan_stat;
    printf("get live channel status=====================\n");
    status = cos_get_live_channel_stat(options, &bucket, &chan_name, &chan_stat, NULL);
    if (status->code != 200) {
        printf("failed to get live channel status\n");
        log_status(status);
    } else {
        content = apr_psprintf(pool, "%.*s", chan_stat.pushflow_status.len, chan_stat.pushflow_status.data);
        printf("status:%s\n", content);
        content = apr_psprintf(pool, "%.*s", chan_stat.connected_time.len, chan_stat.connected_time.data);
        printf("connected_time:%s\n", content);
        content = apr_psprintf(pool, "%.*s", chan_stat.remote_addr.len, chan_stat.remote_addr.data);
        printf("remote_addr:%s\n", content);
    }

    // gen rtmp sign url
    printf("get live channel signed url=====================\n");
    char *rtmp_signed_url = cos_gen_rtmp_signed_url(options, &bucket, &chan_name, 100000, NULL);
    if (!rtmp_signed_url) {
        printf("failed to get live channel signed url\n");
    } else {
        printf("live channel signed url:%s\n", rtmp_signed_url);
    }

    // get history
    cos_list_t live_record_list;
    cos_live_record_content_t *live_record;
    cos_list_init(&live_record_list);
    printf("get live channel history=====================\n");
    status = cos_get_live_channel_history(options, &bucket, &chan_name, &live_record_list, NULL);
    if (status->code != 200) {
        printf("failed to get live channel history");
        log_status(status);
    } else {
        cos_list_for_each_entry(cos_live_record_content_t, live_record, &live_record_list, node) {
            content = apr_psprintf(pool, "%.*s", live_record->start_time.len, live_record->start_time.data);
            printf("start_time:%s\n", content);
            content = apr_psprintf(pool, "%.*s", live_record->end_time.len, live_record->end_time.data);
            printf("end_time:%s\n", content);
            content = apr_psprintf(pool, "%.*s", live_record->remote_addr.len, live_record->remote_addr.data);
            printf("remote_addr:%s\n", content);
            content = apr_psprintf(pool, "%.*s", live_record->request_id.len, live_record->request_id.data);
            printf("request_id:%s\n", content);
        }
    }

    // list channel
    cos_list_live_channel_params_t params;
    cos_live_channel_content_t *live_chan;
    cos_list_init(&params.live_channel_list);
    params.max_keys = 10;
    printf("get live channel list=====================\n");
    status = cos_list_live_channel(options, &bucket, &params, NULL);
    if (status->code != 200) {
        printf("failed to get live channel list\n");
        log_status(status);
    } else {
        cos_list_for_each_entry(cos_live_channel_content_t, live_chan, &params.live_channel_list, node) {
            content = apr_psprintf(pool, "%.*s", live_chan->name.len, live_chan->name.data);
            printf("name:%s\n", content);
            content = apr_psprintf(pool, "%.*s", live_chan->last_modified.len, live_chan->last_modified.data);
            printf("last_modified:%s\n", content);
        }
    }

    // vod
    printf("get live channel vod play list=====================\n");
    cos_string_t playlist;
    cos_str_set(&playlist, "testvod.m3u8");
    uint64_t start = time(NULL);
    status = cos_gen_vod_play_list(options, &bucket, &chan_name, &playlist, start - 1000, start + 1000, NULL);
    if (status->code != 200) {
        printf("failed to get live channel vod play lis\n");
        log_status(status);
    }

    cos_pool_destroy(pool);
}

int main(int argc, char *argv[])
{
    int exit_code = -1;

    
    if (cos_http_io_initialize(NULL, 0) != COSE_OK) {
       exit(1);
    }

    //set log level, default COS_LOG_WARN
    cos_log_set_level(COS_LOG_WARN);

    //set log output, default stderr
    cos_log_set_output(NULL);
    test_live_channel();
    //cos_http_io_deinitialize last
    cos_http_io_deinitialize();

    return exit_code;
}
