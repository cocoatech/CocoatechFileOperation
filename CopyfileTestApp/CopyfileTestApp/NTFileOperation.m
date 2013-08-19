//
//  NTFileOperation.m
//  CopyfileTestApp
//
//  Created by Dragan Milić on 29.7.2013.
//  Copyright (c) 2013 Cocoatech. All rights reserved.
//

#import <copyfile.h>
#import <fts.h>
#import <libgen.h>
#import <sys/stat.h>
#import <sys/mount.h>
#import <sys/xattr.h>
#import <sys/time.h>
#import <membership.h>
#import "NTFileOperation.h"

#define XATTR_MAXSIZE 67108864
#define BIG_FILE_SIZE 4294967295

@interface NTFileOperation ()

@property (nonatomic, retain) NSMutableDictionary *directoryResponses;

@end

typedef struct {
    void                *operation;
    BOOL                copy;
    BOOL                usrcopy;
    dispatch_queue_t    queue;
    dispatch_source_t   timer;
    double              interval;
    BOOL                enabled;
    uint64_t            total_bytes;
    uint64_t            completed_bytes;
    uint64_t            current_bytes;
    uint64_t            start_bytes;
    uint64_t            start_time;
    uint64_t            total_objects;
    uint64_t            completed_objects;
    char                **skip_paths;
    long                skip_pos;
    char                **replace_paths;
    long                replace_pos;
    char                **keepboth_src_paths;
    long                keepboth_src_pos;
    char                **keepboth_dst_paths;
    long                keepboth_dst_pos;
    uint64_t            errcnt;
} op_status;


int access_read(const char *path, struct stat *st)
{
    int result = 0;
    
    if (S_ISLNK(st->st_mode))
    {
        // We can't use access() since it follows symlinks. We have to do all the work.
        uid_t userid = getuid();
        
        if (st->st_uid == userid)
            result = !((st->st_mode & S_IRUSR) == S_IRUSR);
        else
        {
            uuid_t useruuid, groupuuid;
            gid_t groupid = getgid();
            
            int is_member = 0;
            
            if ((mbr_uid_to_uuid(userid, useruuid) == 0) && (mbr_gid_to_uuid(st->st_gid, groupuuid) == 0))
                mbr_check_membership(useruuid, groupuuid, &is_member);
            
            if (st->st_gid == groupid || is_member)
                result = !((st->st_mode & S_IRGRP) == S_IRGRP);
            else
                result = !((st->st_mode & S_IROTH) == S_IROTH);
        }
        
        if (result != 0)
            errno = EACCES;
    }
    else
        result = access(path, _READ_OK | _REXT_OK);
    
    return result;
}

int access_delete(const char *path, struct stat *st)
{
    int result = 0;
    
    if (S_ISLNK(st->st_mode))
    {
        // We can't use access() since it follows symlinks. We have to do all the work.
        
        // First check it it's locked.
        result = st->st_flags & UF_IMMUTABLE;
        
        if (result == 0)
        {
            // Not locked. The parent folder must allow for removing of files/subidrectories.
            char *parent_path = dirname((char *)path);
            result = access(path, _RMFILE_OK);
            
            if (result == 0)
            {
                // In addition, if the parent folder has 'sticky bit' set, either the parent folder or the symlink (or both) must be owned by the current user.
                struct stat prst;
                
                if (lstat(parent_path, &prst) == 0)
                {
                    if ((prst.st_mode & S_ISVTX) != 0)
                    {
                        result = -1;
                        errno = EACCES;
                        
                        uid_t userid = getuid();
                        
                        if ((prst.st_uid == userid) || (st->st_uid == userid))
                        {
                            result = 0;
                            errno = 0;
                        }
                    }
                }
                else
                    result = -1;
            }
        }
        else
        {
            result = -1;
            errno = EPERM;
        }
    }
    else if (S_ISDIR(st->st_mode))
    {
        // Test if dir hierarchy is deletable.
        char *paths[] = { (char *)path, 0 };
        
        FTS *tree = fts_open(paths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
        
        if (tree)
        {
            FTSENT *node;
            
            while ((result == 0) && (node = fts_read(tree)))
            {
                if (node->fts_info != FTS_DP)
                {
                    if (node->fts_info == FTS_ERR || node->fts_info == FTS_DNR || node->fts_info == FTS_NS)
                        result = -1;
                    else if (node->fts_info == FTS_DC || node->fts_info == FTS_INIT || node->fts_info == FTS_NSOK)
                    {
                        result = -1;
                        errno = EBADF;
                    }
                    else if (node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE)
                        result = access_delete(node->fts_path, node->fts_statp);
                    else
                        result = access(node->fts_path, _DELETE_OK);
                }
            }
            
            fts_close(tree);
        }
        else
            result = -1;
    }
    else
        result = access(path, _DELETE_OK);
    
    return result;
}

int access_move(const char *path, struct stat *st)
{
    int result = 0;
    
    if (S_ISLNK(st->st_mode))
    {
        // We can't use access() since it follows symlinks. We have to do all the work.
        
        // First check it it's locked.
        result = st->st_flags & UF_IMMUTABLE;
        
        if (result == 0)
        {
            // Not locked. The parent folder must allow for removing of files/subidrectories.
            char *parent_path = dirname((char *)path);
            result = access(path, _RMFILE_OK);
            
            if (result == 0)
            {
                // In addition, if the parent folder has 'sticky bit' set, either the parent folder or the symlink (or both) must be owned by the current user.
                struct stat prst;
                
                if (lstat(parent_path, &prst) == 0)
                {
                    if ((prst.st_mode & S_ISVTX) != 0)
                    {
                        result = -1;
                        errno = EACCES;
                        
                        uid_t userid = getuid();
                        
                        if ((prst.st_uid == userid) || (st->st_uid == userid))
                        {
                            result = 0;
                            errno = 0;
                        }
                    }
                }
                else
                    result = -1;
            }
        }
        else
        {
            result = -1;
            errno = EPERM;
        }
    }
    else if (S_ISDIR(st->st_mode))
        result = access(path, _DELETE_OK | _APPEND_OK);
    else
        result = access(path, _DELETE_OK);
    
    return result;
}

NTFileConflictResolution delegate_conflict(const char *src, const char *dst, char **prop_dst, void *ctx)
{
    __block NTFileConflictResolution result = NTFileConflictResolutionReplace;
    
    op_status *status = (op_status *)ctx;
    NTFileOperation *operation = (NTFileOperation *)status->operation;
    SEL delegateMethod = NULL;
    
    if (status->usrcopy)
        delegateMethod = @selector(fileOperation:conflictCopyingItemAtURL:toURL:proposedURL:);
    else
        delegateMethod = @selector(fileOperation:conflictMovingItemAtURL:toURL:proposedURL:);
    
    if ([[operation delegate] respondsToSelector:delegateMethod])
    {
        NSURL *srcURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:src length:strlen(src)]];
        NSURL *dstURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:dst length:strlen(dst)]];
        __block NSURL *propURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:*prop_dst length:strlen(*prop_dst)]];
        
        dispatch_group_t group =  dispatch_group_create();
        
        dispatch_group_async(group, status->queue, ^
        {
            @try
            {
                if (status->usrcopy)
                    result = [[operation delegate] fileOperation:operation conflictCopyingItemAtURL:srcURL toURL:dstURL  proposedURL:&propURL];
                else
                    result = [[operation delegate] fileOperation:operation conflictMovingItemAtURL:srcURL toURL:dstURL  proposedURL:&propURL];
                
            }
            @catch (NSException *e)
            {
                NSLog(@"-[%@ %@] exception (calling queue): %@", NSStringFromClass([operation class]), NSStringFromSelector(delegateMethod), e);
            }
        });
        
        dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
        dispatch_release(group);
        
        if (propURL != nil)
        {
            const char *new_dst = [[propURL path] fileSystemRepresentation];
            
            if (strcmp(new_dst, dst) != 0 && strcmp(new_dst, *prop_dst) != 0)
            {
                free(*prop_dst);
                asprintf(prop_dst, "%s", new_dst);
            }
        }
    }

    return result;
}

BOOL delegate_error(NTFileOperationStage stage, const char *src, const char *dst, void *ctx)
{
    __block BOOL result = YES;
    
    op_status *status = (op_status *)ctx;
    NTFileOperation *operation = (NTFileOperation *)status->operation;
    
    int error_code = errno;
    SEL delegateMethod = NULL;
    
    if (stage == NTFileOperationStageRunning)
    {
        if (status->usrcopy)
            delegateMethod = @selector(fileOperation:shouldProceedAfterError:copyingItemAtURL:toURL:);
        else
            delegateMethod = @selector(fileOperation:shouldProceedAfterError:movingItemAtURL:toURL:);
    }
    else
        delegateMethod = @selector(fileOperation:shouldProceedAfterError:preflightingItemAtURL:);
    
    if ([[operation delegate] respondsToSelector:delegateMethod])
    {
        NSURL *srcURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:src length:strlen(src)]];
        
        NSURL *dstURL = nil;
        NSError *error;
        
        if (stage == NTFileOperationStageRunning)
        {
            dstURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:dst length:strlen(dst)]];
            
            error = [NSError errorWithDomain:NSPOSIXErrorDomain
                                        code:error_code
                                    userInfo:[NSDictionary dictionaryWithObjectsAndKeys:
                                              [NSString stringWithUTF8String:strerror(error_code)], NSLocalizedDescriptionKey,
                                              [NSDictionary dictionaryWithObjectsAndKeys:[srcURL path], @"SourceURL", [dstURL path], @"DestinationURL", nil], NSLocalizedFailureReasonErrorKey,
                                              [NSArray arrayWithObjects:@"Skip", @"Quit", nil], NSLocalizedRecoveryOptionsErrorKey, nil]];
        }
        else
            error = [NSError errorWithDomain:NSPOSIXErrorDomain
                                        code:error_code
                                    userInfo:[NSDictionary dictionaryWithObjectsAndKeys:
                                              [NSString stringWithUTF8String:strerror(error_code)], NSLocalizedDescriptionKey,
                                              [NSDictionary dictionaryWithObject:[srcURL path] forKey:@"SourceURL"], NSLocalizedFailureReasonErrorKey,
                                              [NSArray arrayWithObjects:@"Skip", @"Quit", nil], NSLocalizedRecoveryOptionsErrorKey, nil]];
 
        dispatch_group_t group =  dispatch_group_create();
        
        dispatch_group_async(group, status->queue, ^
        {
            @try
            {
                if (stage == NTFileOperationStageRunning)
                {
                    if (status->usrcopy)
                        result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error copyingItemAtURL:srcURL toURL:dstURL];
                    else
                        result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error movingItemAtURL:srcURL toURL:dstURL];
                }
                else
                    result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error preflightingItemAtURL:srcURL];
            }
            @catch (NSException *e)
            {
                NSLog(@"-[%@ %@] exception (calling queue): %@", NSStringFromClass([operation class]), NSStringFromSelector(delegateMethod), e);
            }
        });
        
        dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
        dispatch_release(group);
    }
    
    return result;
}

BOOL delegate_progress(NTFileOperationStage stage, const char *src, const char *dst, void *ctx)
{
    __block BOOL result = YES;
    op_status *status = (op_status *)ctx;
    
    if (status->enabled)
    {
        NTFileOperation *operation = (NTFileOperation *)status->operation;
        SEL delegateMethod = @selector(fileOperation:shouldProceedOnProgressInfo:);
        
        if ([[operation delegate] respondsToSelector:delegateMethod])
        {
            NSDictionary *info;
            
            if (stage == NTFileOperationStageRunning)
            {
                struct timeval currtimeval;
                gettimeofday(&currtimeval, NULL);
                uint64_t curr_time = currtimeval.tv_sec * 1000000 + currtimeval.tv_usec;
                
                uint64_t throughput = (status->completed_bytes - status->start_bytes) * 1000000 / (curr_time - status->start_time);
                
                info = [NSDictionary dictionaryWithObjectsAndKeys:
                        [NSNumber numberWithUnsignedInteger:NTFileOperationStageRunning], NTFileOperationStageKey,
                        [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:src length:strlen(src)]], NTFileOperationSourceItemKey,
                        [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:dst length:strlen(dst)]], NTFileOperationDestinationItemKey,
                        [NSNumber numberWithUnsignedLongLong:status->total_bytes], NTFileOperationTotalBytesKey,
                        [NSNumber numberWithUnsignedLongLong:status->completed_bytes], NTFileOperationCompletedBytesKey,
                        [NSNumber numberWithUnsignedLongLong:status->total_objects], NTFileOperationTotalObjectsKey,
                        [NSNumber numberWithUnsignedLongLong:status->completed_objects], NTFileOperationCompletedObjectsKey,
                        [NSNumber numberWithUnsignedLongLong:throughput], NTFileOperationThroughputKey, nil];
            }
            else
                info = [NSDictionary dictionaryWithObjectsAndKeys:
                        [NSNumber numberWithUnsignedInteger:stage], NTFileOperationStageKey,
                        [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:src length:strlen(src)]], NTFileOperationSourceItemKey,
                        [NSNumber numberWithUnsignedLongLong:status->total_objects], NTFileOperationTotalObjectsKey, nil];
            
            dispatch_group_t group =  dispatch_group_create();
            
            dispatch_group_async(group, status->queue, ^
            {
                @try
                {
                    result = [[operation delegate] fileOperation:operation shouldProceedOnProgressInfo:info];
                }
                @catch (NSException *e)
                {
                    NSLog(@"-[%@ %@] exception (calling queue): %@", NSStringFromClass([operation class]), NSStringFromSelector(delegateMethod), e);
                }
            });
            
            dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
            dispatch_release(group);
            
            status->enabled = NO;
            
            dispatch_time_t pop_time = dispatch_time(DISPATCH_TIME_NOW, (int64_t)(status->interval * NSEC_PER_SEC));
            dispatch_source_set_timer(status->timer, pop_time, DISPATCH_TIME_FOREVER, 0);
            
            dispatch_source_set_event_handler(status->timer, ^
            {
                @try
                {
                    status->enabled = YES;
                }
                @catch (NSException *e)
                {
                    NSLog(@"-[%@ %@] exception (timer event queue): %@", NSStringFromClass([operation class]), NSStringFromSelector(delegateMethod), e);
                }
            });
        }
        else
            status->enabled = NO;
    }
    
    return result;
}

int delete(char * const *paths, void *ctx)
{
    int result = 0;
    
    FTS *tree = fts_open(paths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
    
    if (tree)
    {
        char *prefix = malloc(sizeof(char) * 3);
        FTSENT *node;
        
        while ((result == 0) && (node = fts_read(tree)))
        {
            
            if (node->fts_info == FTS_F || node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE || node->fts_info == FTS_DEFAULT)
            {
                if (node->fts_info == FTS_F)
                {
                    strncpy(prefix, node->fts_name, 2);
                    prefix[2] = '\0';
                    
                    if (strcmp(prefix, "._") == 0)
                    {
                        fts_set(tree, node, FTS_SKIP);
                        continue;
                    }
                }
                
                result = unlink(node->fts_path);
            }
            else if (node->fts_info == FTS_DP)
                result = rmdir(node->fts_path);
            else if ((node->fts_info == FTS_DNR) || (node->fts_info == FTS_ERR) || (node->fts_info == FTS_NS))
                result = -1;
            else if ((node->fts_info == FTS_DC) || (node->fts_info == FTS_INIT) || (node->fts_info == FTS_NSOK) || (node->fts_info == FTS_W))
            {
                result = -1;
                errno = EBADF;
            }
            
            if (result == 0 && !delegate_progress(NTFileOperationStageRunning, node->fts_path, node->fts_path, ctx))
            {
                result = -1;
                errno = ECANCELED;
            }
        }
        
        free(prefix);
        fts_close(tree);
    }
    
    if (result == 0 && errno != 0)
    {
        // This happens if fts_read() or fts_open() fails.
        result = -1;
    }
    
    return result;
}

#define UPDATE_COPY_PREFLIGHT_INFO { status->total_objects++;\
\
    if (node->fts_info != FTS_D)\
    {\
        status->total_bytes += (node->fts_statp)->st_size;\
        \
        ssize_t bytes_cnt = getxattr(node->fts_path, XATTR_RESOURCEFORK_NAME, NULL, XATTR_MAXSIZE, 0, XATTR_NOFOLLOW);\
        \
        if (bytes_cnt > -1)\
            status->total_bytes += bytes_cnt;\
    }\
}

#define SKIP_FILE_PATH(x) { size_t len = status->skip_pos + strlen(x) + 1;\
\
    status->skip_paths = realloc(status->skip_paths, sizeof(char *) * len);\
    char *skip_path = (char *)status->skip_paths + status->skip_pos;\
\
    strcpy(skip_path, x);\
    status->skip_pos = len;\
}

int preflight_copymove(char * const *paths, const char *dst, void *ctx)
{
    int result = 0;
    struct stat st;
    
    if ((result = lstat(dst, &st)) == 0)
    {
        if (!S_ISDIR(st.st_mode))
        {
            result = -1;
            errno = ENOTDIR;
        }
    }
    
    if (result == 0)
        result = access(dst, _WRITE_OK | _APPEND_OK);
    
    op_status *status = (op_status *)ctx;
    
    if (result == 0)
    {
        BOOL supportsBigFiles = YES;
        
        long max_fsizebits = pathconf(dst, _PC_FILESIZEBITS);
        
        if (max_fsizebits == -1)
        {
            struct statfs stfs;
            
            if (statfs(dst, &stfs) == 0)
            {
                struct attrlist attrlst;
                
                attrlst.bitmapcount = ATTR_BIT_MAP_COUNT;
                attrlst.reserved = 0;
                attrlst.commonattr = ATTR_CMN_RETURNED_ATTRS;
                attrlst.volattr = ATTR_VOL_INFO | ATTR_VOL_CAPABILITIES;
                attrlst.dirattr = 0;
                attrlst.fileattr = 0;
                attrlst.forkattr = 0;
                
                void *attrbuf = malloc(56);
                
                if (getattrlist(stfs.f_mntonname, &attrlst, attrbuf, 60, FSOPT_NOFOLLOW ) == 0)
                {
                    u_int32_t vol_cap_format = *((u_int32_t *)attrbuf + 6);
                    u_int32_t vol_cap_format_valid = *((u_int32_t *)attrbuf + 10);
                    
                    if (vol_cap_format_valid & VOL_CAP_FMT_2TB_FILESIZE)
                        supportsBigFiles = vol_cap_format & VOL_CAP_FMT_2TB_FILESIZE;
                }
                
                free(attrbuf);
            }
        }
        else
            supportsBigFiles = (max_fsizebits == 64);
        
        FTSENT *node;
        FTS *tree = fts_open(paths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
        
        if (tree)
        {
            while ((node = fts_read(tree)))
            {
                if (node->fts_info == FTS_F || node->fts_info == FTS_D || node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE || node->fts_info == FTS_DEFAULT)
                {
                    if (node->fts_statp->st_dev != st.st_dev)
                    {
                        status->copy = YES;
                        break;
                    }
                    
                    fts_set(tree, node, FTS_SKIP);
                    
                }
            }
        }
        
        fts_close(tree);
        
        char path_sep = '/';
        
        tree = fts_open(paths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
        
        if (tree)
        {
            short fts_level = -1;
            short replace_level = SHRT_MAX;
            short keepboth_level = SHRT_MAX;
            
            char *dst_path;
            asprintf(&dst_path, "%s", dst);
            
            char *prefix = malloc(sizeof(char) * 3);
            
            FTSENT *node;
            
            while ((result == 0) && (node = fts_read(tree)))
            {
                if (node->fts_info == FTS_F || node->fts_info == FTS_D || node->fts_info == FTS_DP || node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE || node->fts_info == FTS_DEFAULT)
                {
                    if (node->fts_info == FTS_F)
                    {
                        strncpy(prefix, node->fts_name, 2);
                        prefix[2] = '\0';
                        
                        if (strcmp(prefix, "._") == 0)
                        {
                            fts_set(tree, node, FTS_SKIP);
                            continue;
                        }
                    }
                    
                    char *temp_path;
                    asprintf(&temp_path, "%s", dst_path);
                    
                    free(dst_path);
                    
                    if (node->fts_level > fts_level)
                        asprintf(&dst_path, "%s%c%s", temp_path, path_sep, node->fts_name);
                    else if (node->fts_level < fts_level)
                        asprintf(&dst_path, "%s", dirname(temp_path));
                    else
                    {
                        char *dir_path;
                        
                        asprintf(&dir_path, "%s", dirname(temp_path));
                        asprintf(&dst_path, "%s%c%s", dir_path, path_sep, node->fts_name);
                        
                        free(dir_path);
                    }
                    
                    free(temp_path);
                    
                    fts_level = node->fts_level;
                    
                    if (fts_level <= replace_level)
                        replace_level = SHRT_MAX;
                    
                    if (fts_level <= keepboth_level)
                        keepboth_level = SHRT_MAX;
                    
                    if (node->fts_info != FTS_DP)
                    {
                        char *dstdirpath;
                        char *srcdirpath;
                        
                        asprintf(&dstdirpath, "%s%c", dst, path_sep);
                        asprintf(&srcdirpath, "%s%c", node->fts_path, path_sep);
                        
                        if (strstr(dstdirpath, srcdirpath) != NULL)
                        {
                            result = -1;
                            errno = EINVAL;
                        }
                        
                        free(dstdirpath);
                        free(srcdirpath);
                        
                        if (!status->copy && strcmp(node->fts_path, dst_path) == 0)
                        {
                            result = -1;
                            errno = EINVAL;
                        }
                    }
                    
                    if (node->fts_info == FTS_F && !supportsBigFiles && node->fts_statp->st_size > BIG_FILE_SIZE)
                    {
                        result = -1;
                        errno = EFBIG;
                    }
                    
                    if (result == 0)
                    {
                        if (node->fts_info != FTS_DP)
                        {
                            // Not closing dir, so all other files (inculuding opening dir).
                            struct stat st;
                            
                            if (replace_level == SHRT_MAX && keepboth_level == SHRT_MAX && lstat(dst_path, &st) == 0)
                            {
                                // Dest file exists!!!
                                // Create proposed new destination name.
                                char *extension = NULL;
                                char *dot = strrchr(node->fts_name, '.');
                                
                                if (dot && strlen(dot) > 1 && strlen(node->fts_name) > strlen(dot))
                                    extension = dot + 1;
                                
                                size_t noextlen;
                                
                                if (extension)
                                    noextlen = strlen(dst_path) - strlen(extension) - 1;
                                else
                                    noextlen = strlen(dst_path);
                                
                                char *new_dst_noext = malloc(sizeof(char) * (noextlen + 1));
                                strncpy(new_dst_noext, dst_path, noextlen);
                                new_dst_noext[noextlen] = '\0';
                                
                                char *new_dst;
                                
                                if (extension)
                                    asprintf(&new_dst, "%s%s.%s", new_dst_noext, " added", extension);
                                else
                                    asprintf(&new_dst, "%s%s", new_dst_noext, " added");
                                
                                free(new_dst_noext);
                                
                                NTFileConflictResolution resolution = delegate_conflict(node->fts_path, dst_path, &new_dst, ctx);
                                
                                switch (resolution)
                                {
                                    case NTFileConflictResolutionQuit:
                                    {
                                        result = -1;
                                        errno = ECANCELED;
                                        break;
                                    }
                                    case NTFileConflictResolutionSkip:
                                    {
                                        fts_set(tree, node, FTS_SKIP);
                                        SKIP_FILE_PATH(node->fts_path)
                                        break;
                                    }
                                    case NTFileConflictResolutionReplace:
                                    {
                                        if (strcmp(node->fts_path, dst_path) == 0)
                                        {
                                            result = -1;
                                            errno = EINVAL;
                                        }
                                        
                                        if (result == 0)
                                        {
                                            if (!status->copy)
                                            {
                                                fts_set(tree, node, FTS_SKIP);
                                                result = access_move(node->fts_path, node->fts_statp);
                                            }
                                            else
                                                result = access_read(node->fts_path, node->fts_statp);
                                            
                                            if (result == 0)
                                                result = access_delete(dst_path, &st);
                                            
                                            if (result == 0)
                                            {
                                                if (status->copy)
                                                    UPDATE_COPY_PREFLIGHT_INFO
                                                
                                                size_t len = status->replace_pos + strlen(node->fts_path) + 1;
                                                
                                                status->replace_paths = realloc(status->replace_paths, sizeof(char *) * len);
                                                char *replace_path = (char *)status->replace_paths + status->replace_pos;
                                                
                                                strcpy(replace_path, node->fts_path);
                                                status->replace_pos = len;
                                                
                                                replace_level = fts_level;
                                            }
                                        }
                                        break;
                                    }
                                    case NTFileConflictResolutionKeepBoth:
                                    {
                                        if (!status->copy)
                                        {
                                            fts_set(tree, node, FTS_SKIP);
                                            result = access_move(node->fts_path, node->fts_statp);
                                        }
                                        else
                                            result = access_read(node->fts_path, node->fts_statp);
                                        
                                        if ((result == 0) && (node->fts_info == FTS_D))
                                            result = access(dirname((dst_path)), _WRITE_OK | _APPEND_OK);
                                        
                                        if (result == 0)
                                        {
                                            if (status->copy)
                                                UPDATE_COPY_PREFLIGHT_INFO
                                                
                                            size_t len;
                                            char *keepbothpath;
                                            
                                            len = status->keepboth_src_pos + strlen(node->fts_path) + 1;
                                            status->keepboth_src_paths = realloc(status->keepboth_src_paths, sizeof(char *) * len);
                                            keepbothpath = (char *)status->keepboth_src_paths + status->keepboth_src_pos;
                                            strcpy(keepbothpath, node->fts_path);
                                            status->keepboth_src_pos = len;
                                            
                                            len = status->keepboth_dst_pos + strlen(new_dst) + 1;
                                            status->keepboth_dst_paths = realloc(status->keepboth_dst_paths, sizeof(char *) * len);
                                            keepbothpath = (char *)status->keepboth_dst_paths + status->keepboth_dst_pos;
                                            strcpy(keepbothpath, new_dst);
                                            status->keepboth_dst_pos = len;
                                            
                                            keepboth_level = fts_level;
                                        }
                                        break;
                                    }
                                    case NTFileConflictResolutionMerge:
                                    default:
                                    {
                                        if (status->copy)
                                            result = access_read(node->fts_path, node->fts_statp);
                                        else
                                            result = access_delete(node->fts_path, node->fts_statp);
                                        
                                        if ((result == 0) && (node->fts_info == FTS_D))
                                            result = access(dst_path, _WRITE_OK | _APPEND_OK);
                                        
                                        if (status->copy && result == 0)
                                            UPDATE_COPY_PREFLIGHT_INFO
                                            
                                        break;
                                    }
                                }
                                
                                free(new_dst);
                            }
                            else if (replace_level != SHRT_MAX || keepboth_level != SHRT_MAX || errno == ENOENT)
                            {
                                // Dest file doesn't exist!!!
                                if (!status->copy)
                                {
                                    fts_set(tree, node, FTS_SKIP);
                                    result = access_move(node->fts_path, node->fts_statp);
                                }
                                else
                                    result = access_read(node->fts_path, node->fts_statp);
                                
                                if (status->copy && result == 0)
                                    UPDATE_COPY_PREFLIGHT_INFO
                            }
                            else
                                result = -1;
                        }
                    }
                }
                else if ((node->fts_info == FTS_DNR) || (node->fts_info == FTS_ERR) || (node->fts_info == FTS_NS))
                    result = -1;
                else if ((node->fts_info == FTS_DC) || (node->fts_info == FTS_INIT) || (node->fts_info == FTS_NSOK) || (node->fts_info == FTS_W))
                {
                    result = -1;
                    errno = EBADF;
                }
                
                if (result == 0)
                {
                    if (!delegate_progress(NTFileOperationStagePreflighting, node->fts_path, NULL, ctx))
                    {
                        result = -1;
                        errno = ECANCELED;
                    }
                }
                else if (errno != ECANCELED)
                {
                    if (delegate_error(NTFileOperationStagePreflighting, node->fts_path, NULL, ctx))
                    {
                        result = 0;
                        fts_set(tree, node, FTS_SKIP);
                        SKIP_FILE_PATH(node->fts_path)
                    }
                    else
                        errno = ECANCELED;
                }
            }
            
            free(dst_path);
            free(prefix);
            
            fts_close(tree);
        }
        
        if (result == 0 && errno != 0)
        {
            // This happens if fts_read() or fts_open() fails.
            if (delegate_error(NTFileOperationStagePreflighting, paths[0], NULL, ctx))
            {
                result = 0;
                SKIP_FILE_PATH(paths[0])
            }
            else
            {
                result = -1;
                errno = ECANCELED;
            }
        }
        
    }
    else
        delegate_error(NTFileOperationStagePreflighting, paths[0], dst, ctx);
    
    char *path;
    
    status->skip_paths = realloc(status->skip_paths, sizeof(char *) * (status->skip_pos + 1));
    path = (char *)status->skip_paths + status->skip_pos;
    path[0] = '\0';
    
    status->replace_paths = realloc(status->replace_paths, sizeof(char *) * (status->replace_pos + 1));
    path = (char *)status->replace_paths + status->replace_pos;
    path[0] = '\0';
    
    status->keepboth_src_paths = realloc(status->keepboth_src_paths, sizeof(char *) * (status->keepboth_src_pos + 1));
    path = (char *)status->keepboth_src_paths + status->keepboth_src_pos;
    path[0] = '\0';
    
    status->keepboth_dst_paths = realloc(status->keepboth_dst_paths, sizeof(char *) * (status->keepboth_dst_pos + 1));
    path = (char *)status->keepboth_dst_paths + status->keepboth_dst_pos;
    path[0] = '\0';
    
    return result;
}

int progress_copymove(int what, int stage, copyfile_state_t state, const char *src, const char *dst, void *ctx)
{
    int result = COPYFILE_CONTINUE;
    
    op_status *status = (op_status *)ctx;
    NTFileOperation *operation = (NTFileOperation *)status->operation;
    
    if (stage == COPYFILE_START)
    {
        if (what == COPYFILE_RECURSE_DIR || what == COPYFILE_RECURSE_FILE)
        {
            char *res_path;
            
            res_path = (char *)status->skip_paths + status->skip_pos;
            if (res_path[0] != '\0' && strcmp(src, res_path) == 0)
            {
                // SKIPED FILE!!!
                result = COPYFILE_SKIP;
                status->skip_pos += strlen(res_path) + 1;
            }
            else
            {
                struct stat st;
                
                // See if it exists and if so, ask delegate.
                if (lstat(dst, &st) == 0)
                {
                    res_path = (char *)status->replace_paths + status->replace_pos;
                    if (res_path[0] != '\0' && strcmp(src, res_path) == 0)
                    {
                        // REPLACED FILE!!!
                        char *paths[] = { (char *)dst, 0 };
                        
                        if (delete(paths, ctx) != 0)
                        {
                            result = progress_copymove(what, COPYFILE_ERR, state, src, dst, ctx);
                            
                            if (result == COPYFILE_CONTINUE)
                                result = COPYFILE_SKIP;
                        }
                        else
                        {
                            if (!status->copy)
                            {
                                if (rename(src, dst) != 0)
                                {
                                    result = progress_copymove(what, COPYFILE_ERR, state, src, dst, ctx);
                                    
                                    if (result == COPYFILE_CONTINUE)
                                        result = COPYFILE_SKIP;
                                }
                                else
                                    result = COPYFILE_SKIP;
                            }
                            else
                                result = COPYFILE_CONTINUE;
                        }
                        
                        status->replace_pos += strlen(res_path) + 1;
                    }
                    
                    res_path = (char *)status->keepboth_src_paths + status->keepboth_src_pos;
                    if (res_path[0] != '\0' && strcmp(src, res_path) == 0)
                    {
                        // KEEP BOTH FILEs!!!
                        char *new_dst = (char *)status->keepboth_dst_paths + status->keepboth_dst_pos;
                        
                        if (status->copy)
                        {
                            copyfile_callback_t callback = progress_copymove;
                            copyfile_state_t state = copyfile_state_alloc();
                            
                            copyfile_state_set(state, COPYFILE_STATE_STATUS_CB, callback);
                            copyfile_state_set(state, COPYFILE_STATE_STATUS_CTX, ctx);
                            copyfile_flags_t flags = COPYFILE_ALL | COPYFILE_RECURSIVE | COPYFILE_NOFOLLOW;
                            
                            copyfile(src, new_dst, state, flags);
                            copyfile_state_free(state);
                            
                            result = COPYFILE_SKIP;
                        }
                        else
                        {
                            if (rename(src, new_dst) != 0)
                            {
                                result = progress_copymove(what, COPYFILE_ERR, state, src, dst, ctx);
                                
                                if (result == COPYFILE_CONTINUE)
                                    result = COPYFILE_SKIP;
                            }
                            else
                                result = COPYFILE_SKIP;
                        }
                        
                        status->keepboth_src_pos += strlen(res_path) + 1;
                        status->keepboth_dst_pos += strlen(new_dst) + 1;
                    }
                }
                else
                {
                    if (!status->copy)
                    {
                        if (rename(src, dst) != 0)
                        {
                            result = progress_copymove(what, COPYFILE_ERR, state, src, dst, ctx);
                            
                            if (result == COPYFILE_CONTINUE)
                                result = COPYFILE_SKIP;
                        }
                        else
                            result = COPYFILE_SKIP;
                    }
                    else
                        result = COPYFILE_CONTINUE;
                }
            }
            
            if (!delegate_progress(NTFileOperationStageRunning, src, dst, ctx))
                result = COPYFILE_QUIT;
            
            if (what == COPYFILE_RECURSE_DIR)
                [[operation directoryResponses] setObject:[NSNumber numberWithInt:result] forKey:[NSString stringWithUTF8String:src]];
            
            status->current_bytes = 0;
        }
        else if (what == COPYFILE_RECURSE_DIR_CLEANUP)
            result = [[[operation directoryResponses] objectForKey:[NSString stringWithUTF8String:src]] intValue];
    }
    else if (stage == COPYFILE_FINISH)
    {
        if (what == COPYFILE_COPY_XATTR)
        {
            char *attr_name;
            copyfile_state_get(state, COPYFILE_STATE_XATTRNAME, &attr_name);
            
            if (strcmp(attr_name, XATTR_RESOURCEFORK_NAME) == 0)
            {
                ssize_t bytes_cnt = getxattr(src, attr_name, NULL, XATTR_MAXSIZE, 0, XATTR_NOFOLLOW);
                
                if (bytes_cnt > -1)
                    status->completed_bytes += bytes_cnt;
            }
        }
        else if (what == COPYFILE_RECURSE_DIR_CLEANUP || what == COPYFILE_RECURSE_FILE)
        {
            if (what == COPYFILE_RECURSE_DIR_CLEANUP)
            {
                if (!status->copy)
                {
                    char *paths[] = { (char *)src, 0 };
                    delete(paths, ctx);
                }
            }
            
            status->completed_objects++;
        }
    }
    else if (stage == COPYFILE_PROGRESS)
    {
        off_t bytes_cnt;
        copyfile_state_get(state, COPYFILE_STATE_COPIED, &bytes_cnt);
        
        status->completed_bytes += bytes_cnt - status->current_bytes;
        status->current_bytes = bytes_cnt;
        
        if (!delegate_progress(NTFileOperationStageRunning, src, dst, ctx))
            result = COPYFILE_QUIT;
    }
    else if (stage == COPYFILE_ERR)
    {
        if (errno != ECANCELED)
        {
            status->errcnt++;
            
            if (delegate_error(NTFileOperationStageRunning, src, dst, ctx))
            {
                struct timeval currtimeval;
                gettimeofday(&currtimeval, NULL);
                
                status->start_time = currtimeval.tv_sec * 1000000 + currtimeval.tv_usec;
                status->start_bytes = status->completed_bytes;
                
                result = COPYFILE_SKIP;
            }
            else
                result = COPYFILE_QUIT;
        }
        else
            result = COPYFILE_QUIT;
    }
    
    return result;
}

@interface NTFileOperation (Private)

- (void)checkSources:(NSArray *)theSrcURLs destination:(NSURL *)aDstURL;
- (void)doAsyncCopy:(BOOL)isCopy itemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL;

@end

@implementation NTFileOperation

- (void)dealloc
{
    [self setDelegate:nil];
    [self setDirectoryResponses:nil];
    
    [super dealloc];
}

- (id)init
{
    self = [super init];
    
    [self setStatusChangeInterval:0.4];
    
    return self;
}

- (void)copyAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL
{
    [self copyAsyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURL:aDstURL];
}

- (void)copyAsyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL
{
    [self doAsyncCopy:YES itemsAtURLs:theSrcURLs toURL:aDstURL];
}

- (BOOL)copySyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL error:(NSError **)anError
{
    return [self copySyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURL:aDstURL error:anError];
}

- (BOOL)copySyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL error:(NSError **)anError
{
    [self checkSources:theSrcURLs destination:aDstURL];
    
    BOOL result = YES;
    
    copyfile_state_t state = copyfile_state_alloc();
    copyfile_flags_t flags = COPYFILE_ALL | COPYFILE_RECURSIVE | COPYFILE_NOFOLLOW;
    
    const char *dst = [[aDstURL path] fileSystemRepresentation];
    
    for (NSURL *srcURL in theSrcURLs)
    {
        const char *src = [[srcURL path] fileSystemRepresentation];
        
        if (copyfile(src, dst, state, flags) != 0)
        {
            result = NO;
            
            if (anError)
                *anError = [NSError errorWithDomain:NSPOSIXErrorDomain code:errno userInfo:nil];
            
            break;
        }
    }
    
    copyfile_state_free(state);
    
    return result;
}

- (void)moveAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL
{
    [self moveAsyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURL:aDstURL];
}

- (void)moveAsyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL
{
    [self doAsyncCopy:NO itemsAtURLs:theSrcURLs toURL:aDstURL];
}

- (BOOL)moveSyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL error:(NSError **)anError
{
    return [self moveSyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURL:aDstURL error:anError];
}

- (BOOL)moveSyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL error:(NSError **)anError
{
    [self checkSources:theSrcURLs destination:aDstURL];
    
    BOOL result = YES;
    
    const char *dst = [[aDstURL path] fileSystemRepresentation];
    
    for (NSURL *srcURL in theSrcURLs)
    {
        const char *src = [[srcURL path] fileSystemRepresentation];
        
        if (rename(src, dst) != 0)
        {
            result = NO;
            
            if (anError)
                *anError = [NSError errorWithDomain:NSPOSIXErrorDomain code:errno userInfo:nil];
            
            break;
        }
    }
    
    return result;
}

@end

@implementation NTFileOperation (Private)

- (void)checkSources:(NSArray *)theSrcURLs destination:(NSURL *)aDstURL
{
    if (aDstURL == nil || ![aDstURL isKindOfClass:[NSURL class]])
        [NSException raise:NSInvalidArgumentException format:@"-[%@ %@] exception: destination is not NSURL object.", NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    
    for (id source in theSrcURLs)
    {
        if (![source isKindOfClass:[NSURL class]])
            [NSException raise:NSInvalidArgumentException format:@"-[%@ %@] exception: source is not URL object.", NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
}

- (void)doAsyncCopy:(BOOL)isCopy itemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL
{
    [self checkSources:theSrcURLs destination:aDstURL];
    
    if ([theSrcURLs count] == 0)
        return;
    
    dispatch_queue_t queue = dispatch_get_current_queue();
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^
    {
        @try
        {
            op_status status;
            
            status.operation = (void *)self;
            status.copy = isCopy;
            status.usrcopy = isCopy;
            status.queue = queue;
            status.timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0));
            status.interval = [self statusChangeInterval];
            status.enabled = YES;
            status.total_bytes = 0;
            status.total_objects = 0;
            status.skip_paths = NULL;
            status.skip_pos = 0;
            status.replace_paths = NULL;
            status.replace_pos = 0;
            status.keepboth_src_paths = NULL;
            status.keepboth_src_pos = 0;
            status.keepboth_dst_paths = NULL;
            status.keepboth_dst_pos = 0;
            status.errcnt = 0;
            
            char *paths[[theSrcURLs count] + 1];
            
            int i = 0;
            for (NSURL *srcURL in theSrcURLs)
                paths[i++] = (char *)[[srcURL path] fileSystemRepresentation];
            paths[i] = 0;
            
            const char *dst = [[aDstURL path] fileSystemRepresentation];
            
            dispatch_resume(status.timer);
            
            if (preflight_copymove(paths, dst, (void *)&status) == 0)
            {
                status.enabled = YES;
                status.completed_bytes = 0;
                status.current_bytes = 0;
                status.start_bytes = 0;
                status.completed_objects = 0;
                
                struct timeval start_time;
                gettimeofday(&start_time, NULL);
                status.start_time = start_time.tv_sec * 1000000 + start_time.tv_usec;
                
                status.skip_pos = 0;
                status.replace_pos = 0;
                status.keepboth_src_pos = 0;
                status.keepboth_dst_pos = 0;
                
                [self setDirectoryResponses:[NSMutableDictionary dictionary]];
                
                copyfile_state_t state = copyfile_state_alloc();
                
                copyfile_state_set(state, COPYFILE_STATE_STATUS_CB, progress_copymove);
                copyfile_state_set(state, COPYFILE_STATE_STATUS_CTX, (void *)&status);
                copyfile_flags_t flags = COPYFILE_ALL | COPYFILE_RECURSIVE | COPYFILE_NOFOLLOW;
                
                int result;
                
                for (NSURL *srcURL in theSrcURLs)
                {
                    const char *src = [[srcURL path] fileSystemRepresentation];
                    
                    result = copyfile(src, dst, state, flags);
                    
                    if (result != 0)
                        break;
                }
                
                copyfile_state_free(state);
                
                if (status.copy && !status.usrcopy && result == 0 && status.skip_pos == 0 && status.errcnt == 0)
                    delete(paths, (void *)&status);
                
                dispatch_release(status.timer);
            }
            
            free(status.skip_paths);
            free(status.replace_paths);
            free(status.keepboth_src_paths);
            free(status.keepboth_dst_paths);
            
            SEL delegateMethod = @selector(fileOperation:shouldProceedOnProgressInfo:);
            
            if ([[self delegate] respondsToSelector:delegateMethod])
            {
                NSDictionary *info = [NSDictionary dictionaryWithObjectsAndKeys:
                                      [NSNumber numberWithUnsignedInteger:NTFileOperationStageComplete], NTFileOperationStageKey,
                                      [theSrcURLs lastObject], NTFileOperationSourceItemKey,
                                      aDstURL, NTFileOperationDestinationItemKey,
                                      [NSNumber numberWithUnsignedLongLong:status.total_bytes], NTFileOperationTotalBytesKey,
                                      [NSNumber numberWithUnsignedLongLong:status.completed_bytes], NTFileOperationCompletedBytesKey,
                                      [NSNumber numberWithUnsignedLongLong:status.total_objects], NTFileOperationTotalObjectsKey,
                                      [NSNumber numberWithUnsignedLongLong:status.completed_objects], NTFileOperationCompletedObjectsKey,
                                      [NSNumber numberWithUnsignedLongLong:0], NTFileOperationThroughputKey, nil];
                
                dispatch_group_t group =  dispatch_group_create();
                
                dispatch_group_async(group, status.queue, ^
                {
                    @try
                    {
                        [[self delegate] fileOperation:self shouldProceedOnProgressInfo:info];
                    }
                    @catch (NSException *e)
                    {
                        NSLog(@"-[%@ %@] exception (calling queue): %@", NSStringFromClass([self class]), NSStringFromSelector(delegateMethod), e);
                    }
                });
                
                dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
                dispatch_release(group);
            }
        }
        @catch (NSException *e)
        {
            NSLog(@"-[%@ %@] exception (process queue): %@", NSStringFromClass([self class]), NSStringFromSelector(_cmd), e);
        }
    });
}

@end
