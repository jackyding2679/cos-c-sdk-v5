#include "cos_log.h"
#include "cos_define.h"
#include "cos_string.h"
#include "cos_status.h"
#include "cos_auth.h"
#include "cos_utility.h"
#include "cos_xml.h"
#include "cos_api.h"

cos_status_t *cos_create_live_channel(const cos_request_options_t *options,
                                      const cos_string_t *bucket,
                                      cos_live_channel_configuration_t *config,
                                      cos_list_t *publish_url_list,
                                      cos_list_t *play_url_list,
                                      cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    cos_list_t body;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_live_channel_request(options, bucket, &config->name, HTTP_PUT,
                            &req, query_params, headers, &resp);

    // build body
    build_create_live_channel_body(options->pool, config, &body);
    cos_write_request_body_from_buffer(&body, req);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    // parse result
    res = cos_create_live_channel_parse_from_body(options->pool, &resp->body, 
        publish_url_list, play_url_list);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_put_live_channel_switch(const cos_request_options_t *options,
                                          const cos_string_t *bucket,
                                          const cos_string_t *live_channel,
                                          const cos_string_t *live_channel_switch,
                                          cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");
    apr_table_add(query_params, COS_LIVE_CHANNEL_SWITCH, live_channel_switch->data);
    
    //init headers, forbid 'Expect' and 'Transfer-Encoding' of HTTP
    headers = cos_table_create_if_null(options, headers, 2);
    apr_table_set(headers, "Expect", "");
    apr_table_set(headers, "Transfer-Encoding", "");

    cos_init_live_channel_request(options, bucket, live_channel, HTTP_PUT,
                            &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    
    return s;
}

cos_status_t *cos_get_live_channel_info(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *live_channel,
                                        cos_live_channel_configuration_t *info,
                                        cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init query_params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_live_channel_request(options, bucket, live_channel, HTTP_GET,
                            &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    // parse result
    res = cos_live_channel_info_parse_from_body(options->pool, &resp->body, info);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }
    cos_str_set(&info->name, cos_pstrdup(options->pool, live_channel));

    return s;
}

cos_status_t *cos_get_live_channel_stat(const cos_request_options_t *options,
                                        const cos_string_t *bucket,
                                        const cos_string_t *live_channel,
                                        cos_live_channel_stat_t *stat,
                                        cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");
    apr_table_add(query_params, COS_COMP, COS_LIVE_CHANNEL_STAT);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_live_channel_request(options, bucket, live_channel, HTTP_GET,
                            &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    // parse result
    res = cos_live_channel_stat_parse_from_body(options->pool, &resp->body, stat);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_delete_live_channel(const cos_request_options_t *options,
                                      const cos_string_t *bucket,
                                      const cos_string_t *live_channel,
                                      cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 1);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_live_channel_request(options, bucket, live_channel, HTTP_DELETE,
                            &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

cos_status_t *cos_list_live_channel(const cos_request_options_t *options,
                                    const cos_string_t *bucket,
                                    cos_list_live_channel_params_t *params,
                                    cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 4);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");
    apr_table_add(query_params, COS_PREFIX, params->prefix.data);
    apr_table_add(query_params, COS_MARKER, params->marker.data);
    cos_table_add_int(query_params, COS_MAX_KEYS, params->max_keys);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_bucket_request(options, bucket, HTTP_GET, &req,
                            query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    // parse result
    res = cos_list_live_channel_parse_from_body(options->pool, &resp->body,
        &params->live_channel_list, &params->next_marker, &params->truncated);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_get_live_channel_history(const cos_request_options_t *options,
                                           const cos_string_t *bucket,
                                           const cos_string_t *live_channel,
                                           cos_list_t *live_record_list,
                                           cos_table_t **resp_headers)
{
    int res = COSE_OK;
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 2);
    apr_table_add(query_params, COS_LIVE_CHANNEL, "");
    apr_table_add(query_params, COS_COMP, COS_LIVE_CHANNEL_HISTORY);

    //init headers
    headers = cos_table_create_if_null(options, headers, 0);

    cos_init_live_channel_request(options, bucket, live_channel, HTTP_GET,
                            &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);
    if (!cos_status_is_ok(s)) {
        return s;
    }

    // parse result
    res = cos_live_channel_history_parse_from_body(options->pool, &resp->body, live_record_list);
    if (res != COSE_OK) {
        cos_xml_error_status_set(s, res);
    }

    return s;
}

cos_status_t *cos_gen_vod_play_list(const cos_request_options_t *options,
                                     const cos_string_t *bucket,
                                     const cos_string_t *live_channel,
                                     const cos_string_t *play_list_name,
                                     const int64_t start_time,
                                     const int64_t end_time,
                                     cos_table_t **resp_headers)
{
    cos_status_t *s = NULL;
    cos_http_request_t *req = NULL;
    cos_http_response_t *resp = NULL;
    cos_table_t *query_params = NULL;
    cos_table_t *headers = NULL;
    char *resource = NULL;
    cos_string_t resource_str;

    //init params
    query_params = cos_table_create_if_null(options, query_params, 3);
    apr_table_add(query_params, COS_LIVE_CHANNEL_VOD, "");
    apr_table_add(query_params, COS_LIVE_CHANNEL_START_TIME,
        apr_psprintf(options->pool, "%" APR_INT64_T_FMT, start_time));
    apr_table_add(query_params, COS_LIVE_CHANNEL_END_TIME,
        apr_psprintf(options->pool, "%" APR_INT64_T_FMT, end_time));

    //init headers
    headers = cos_table_create_if_null(options, headers, 1);
    apr_table_set(headers, COS_CONTENT_TYPE, COS_MULTIPART_CONTENT_TYPE);

    resource = apr_psprintf(options->pool, "%s/%s", live_channel->data, play_list_name->data);
    cos_str_set(&resource_str, resource);

    cos_init_live_channel_request(options, bucket, &resource_str, HTTP_POST,
                            &req, query_params, headers, &resp);

    s = cos_process_request(options, req, resp);
    cos_fill_read_response_header(resp, resp_headers);

    return s;
}

char *cos_gen_rtmp_signed_url(const cos_request_options_t *options,
                                      const cos_string_t *bucket,
                                      const cos_string_t *live_channel,
                                      const uint64_t expire_sec,
                                      cos_table_t *params)
{
    cos_string_t signed_url;
    // char *expires_str = NULL;
    // cos_string_t expires_time;
    int res = COSE_OK;
    cos_http_request_t *req = NULL;

    // expires_str = apr_psprintf(options->pool, "%" APR_INT64_T_FMT, expires);
    // cos_str_set(&expires_time, expires_str);
    req = cos_http_request_create(options->pool);
    cos_get_rtmp_uri(options, bucket, live_channel, req);
    res = cos_get_rtmp_signed_url(options, req, expire_sec, params, &signed_url);
    if (res != COSE_OK) {
        return NULL;
    }
    return signed_url.data;
}
