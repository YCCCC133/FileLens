#include <CoreServices/CoreServices.h>
#include <CoreFoundation/CoreFoundation.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static volatile int history_done = 0;
static volatile unsigned long long event_count = 0;
static FSEventStreamEventId max_event_id = 0;
static int quiet_output = 0;

static void callback(ConstFSEventStreamRef stream, void *info, size_t count,
                     void *event_paths,
                     const FSEventStreamEventFlags flags[],
                     const FSEventStreamEventId ids[]) {
    (void)stream;
    (void)info;
    char **paths = event_paths;
    for (size_t i = 0; i < count; i++) {
        if (flags[i] & kFSEventStreamEventFlagHistoryDone) history_done = 1;
        if (ids[i] > max_event_id) max_event_id = ids[i];
        event_count++;
        /* 协议使用 NUL 字节分隔记录，保留文件名中的换行/制表符等真实字符，
           路径不再被改写；Python 端以 stdout.split(b"\0") 解析。 */
        if (!quiet_output) {
            printf("%llu\t%u\t%s%c", (unsigned long long)ids[i],
                   (unsigned int)flags[i], paths[i], '\0');
        }
    }
    fflush(stdout);
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s current VOLUME | changes VOLUME SINCE_ID\n", argv[0]);
        return 2;
    }
    struct stat volume_stat;
    if (stat(argv[2], &volume_stat) != 0) return 3;
    dev_t device = volume_stat.st_dev;
    int current_mode = (argc >= 3 && strcmp(argv[1], "current") == 0);
    if (!current_mode && (argc != 4 || strcmp(argv[1], "changes") != 0)) {
        fprintf(stderr, "usage: %s current [START_ID] | changes VOLUME SINCE_ID\n", argv[0]);
        return 2;
    }

    /* The system-wide event id is safe as a future baseline and is available
       immediately. Replaying a busy volume merely to discover its tail can
       otherwise delay first launch by up to a minute. */
    if (current_mode) {
        printf("%llu%c", (unsigned long long)FSEventsGetCurrentEventId(), '\0');
        return 0;
    }

    /* current 模式: 默认从最早历史开始；可传 START_ID 从已知位点附近
       开始回放，避免重放全部历史事件(外部卷可达数十万条)。 */
    FSEventStreamEventId since = 1;
    if (!current_mode) {
        since = (FSEventStreamEventId)strtoull(argv[3], NULL, 10);
    } else if (argc >= 4) {
        since = (FSEventStreamEventId)strtoull(argv[3], NULL, 10);
    }
    quiet_output = current_mode;
    /* 历史事件可能达数十万行，放大缓冲减少系统调用，避免拖慢回放。 */
    setvbuf(stdout, NULL, _IOFBF, 1 << 20);

    CFStringRef path = CFSTR("/");
    if (!path) return 3;
    const void *values[] = {path};
    CFArrayRef paths = CFArrayCreate(NULL, values, 1, &kCFTypeArrayCallBacks);
    FSEventStreamContext context = {0, NULL, NULL, NULL, NULL};
    FSEventStreamRef stream = FSEventStreamCreateRelativeToDevice(
        NULL, callback, &context, device, paths, since, 0.05,
        kFSEventStreamCreateFlagFileEvents |
        kFSEventStreamCreateFlagNoDefer |
        kFSEventStreamCreateFlagWatchRoot |
        kFSEventStreamCreateFlagIgnoreSelf);
    CFRelease(paths);
    if (!stream) return 4;

    FSEventStreamScheduleWithRunLoop(stream, CFRunLoopGetCurrent(),
                                    kCFRunLoopDefaultMode);
    if (!FSEventStreamStart(stream)) {
        FSEventStreamInvalidate(stream);
        FSEventStreamRelease(stream);
        return 5;
    }
    /* 关键修复：FSEvents 在外部卷(ExFAT)上会把历史日志分成多个批次
       回放，且每一批末尾都会带 HistoryDone 标志。旧实现读到第一批
       就退出，导致 changes 只返回一小段历史、永远追不上最新事件，
       增量刷新会永久错过之后创建/修改的文件。
       正确做法：持续读取，直到连续 2 秒没有新事件（认为已到达当前
       位点），最多 60 秒兜底。 */
    /* GUI 极速刷新已在上层按卷并行，并在游标落后过多时强制全盘兜底。
       极速模式把空闲收敛窗口从 2 秒缩至 0.5 秒，避免活跃系统卷上的
       无关后台事件把一次零变更刷新拖到十几秒。 */
    int turbo = getenv("LYCSEARCH_TURBO") != NULL;
    int idle_limit = turbo ? 5 : 20;
    int max_rounds = turbo ? 100 : 600;
    int idle_rounds = 0;
    unsigned long long last_count = 0;
    int converged = 0;
    for (int i = 0; i < max_rounds; i++) {
        CFRunLoopRunInMode(kCFRunLoopDefaultMode, 0.1, false);
        if (event_count != last_count) {
            last_count = event_count;
            idle_rounds = 0;
        } else if (++idle_rounds >= idle_limit) {
            converged = 1;
            break;
        }
    }
    FSEventStreamStop(stream);
    if (current_mode) {
        printf("%llu%c", (unsigned long long)max_event_id, '\0');
    } else {
        printf("LATEST\t%llu%c", (unsigned long long)max_event_id, '\0');
        /* 上层据此区分“已追到事件流尾部”与“回放超时被截断”：
           空闲收敛才算完整；达到轮询上限仍有事件到来意味着历史
           没读完，调用方应回退全盘扫描，避免基线越过未读事件。 */
        printf("STATUS\t%s%c", converged ? "DONE" : "TIMEOUT", '\0');
    }
    fflush(stdout);
    FSEventStreamInvalidate(stream);
    FSEventStreamRelease(stream);
    return 0;
}
