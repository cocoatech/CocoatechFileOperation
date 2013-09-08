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

typedef struct {
    void                *operation;
    BOOL                async;
    BOOL                copy;
    BOOL                usrcopy;
    BOOL                delete;
    BOOL                overwrite;
    BOOL                skip_perm_err;
    BOOL                move_acr_vol;
    dispatch_queue_t    queue;
    dispatch_source_t   timer;
    double              interval;
    BOOL                enabled;
    char                *src;
    char                *dst;
    uint64_t            total_bytes;
    uint64_t            completed_bytes;
    uint64_t            total_objects;
    uint64_t            completed_objects;
    uint64_t            current_bytes;
    uint64_t            start_bytes;
    uint64_t            start_time;
    uint64_t            throughputs[10];
    int                 *dir_responses;
    uint64_t            dir_resp_cnt;
    char                **skip_paths;
    long                skip_pos;
    char                **replace_paths;
    long                replace_pos;
    char                **keepboth_src_paths;
    long                keepboth_src_pos;
    char                **keepboth_dst_paths;
    long                keepboth_dst_pos;
    int                 err;
    uint64_t            errcnt;
    char                *errpath;
    int                 errreason;
    uint64_t            missing_bytes;
} op_status;

int access_read(const char *);
int access_delete(const char *, off_t *);
int access_move(const char *);
NTFileConflictResolution delegate_conflict(const char *, const char *, char **, void *);
BOOL delegate_error(NTFileOperationStage, const char *, const char *, void *);
BOOL delegate_progress(NTFileOperationStage, const char *, const char *, void *);
int delete(char * const *, int, void *);
int preflight_copymove(char * const *, char * const *, void *);
int preflight_delete(char * const * srcs, void *ctx);
int progress_copymove(int, int, copyfile_state_t, const char *, const char *, void *);
int deletefiles(char * const *paths, void *ctx);


int access_read(const char *path)
{
    int result = 0;
    
    struct stat st;
    
    if ((result = lstat(path, &st)) == 0)
    {
        if (S_ISLNK(st.st_mode))
        {
            // We can't use access() since it follows symlinks. We have to do all the work.
            uid_t userid = getuid();
            
            if (st.st_uid == userid)
                result = !((st.st_mode & S_IRUSR) == S_IRUSR);
            else
            {
                uuid_t useruuid, groupuuid;
                gid_t groupid = getgid();
                
                int is_member = 0;
                
                if ((mbr_uid_to_uuid(userid, useruuid) == 0) && (mbr_gid_to_uuid(st.st_gid, groupuuid) == 0))
                    mbr_check_membership(useruuid, groupuuid, &is_member);
                
                if (st.st_gid == groupid || is_member)
                    result = !((st.st_mode & S_IRGRP) == S_IRGRP);
                else
                    result = !((st.st_mode & S_IROTH) == S_IROTH);
            }
            
            if (result != 0)
                errno = EACCES;
        }
        else
            result = access(path, _READ_OK | _REXT_OK);
    }
    
    return result;
}

int access_delete(const char *path, off_t *size)
{
    int result = 0;
    
    struct stat st;
    
    if ((result = lstat(path, &st)) == 0)
    {
        if (S_ISLNK(st.st_mode))
        {
            // We can't use access() since it follows symlinks. We have to do all the work.
            
            // First check it it's locked.
            result = st.st_flags & UF_IMMUTABLE;
            
            if (result == 0)
            {
                // Not locked. The parent folder must allow for removing of files/subidrectories.
                char *parent_path = dirname((char *)path);
                result = access(parent_path, _RMFILE_OK);
                
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
                            
                            if ((prst.st_uid == userid) || (st.st_uid == userid))
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
            
            if (size != NULL)
                *size += st.st_size;
        }
        else if (S_ISDIR(st.st_mode))
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
                            result = access_delete(node->fts_path, size);
                        else
                        {
                            result = access(node->fts_path, _DELETE_OK);
                            
                            if (size != NULL)
                                *size += node->fts_statp->st_size;
                        }
                    }
                }
                
                fts_close(tree);
            }
            else
                result = -1;
        }
        else
        {
            result = access(path, _DELETE_OK);
            
            if (size != NULL)
                *size += st.st_size;
        }
        
    }
    
    return result;
}

int access_move(const char *path)
{
    int result = 0;
    
    struct stat st;
    
    if ((result = lstat(path, &st)) == 0)
    {
        if (S_ISLNK(st.st_mode))
        {
            // We can't use access() since it follows symlinks. We have to do all the work.
            
            // First check it it's locked.
            result = st.st_flags & UF_IMMUTABLE;
            
            if (result == 0)
            {
                // Not locked. The parent folder must allow for removing of files/subidrectories.
                char *parent_path = dirname((char *)path);
                result = access(parent_path, _RMFILE_OK);
                
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
                            
                            if ((prst.st_uid == userid) || (st.st_uid == userid))
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
        else if (S_ISDIR(st.st_mode))
            result = access(path, _DELETE_OK | _APPEND_OK);
        else
            result = access(path, _DELETE_OK);
    }
    
    return result;
}

NTFileConflictResolution delegate_conflict(const char *src, const char *dst, char **prop_dst, void *ctx)
{
    __block NTFileConflictResolution result;
    
    op_status *status = (op_status *)ctx;
    
    if (status->async)
    {
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
                        result = [[operation delegate] fileOperation:operation conflictCopyingItemAtURL:srcURL toURL:dstURL proposedURL:&propURL];
                    else
                        result = [[operation delegate] fileOperation:operation conflictMovingItemAtURL:srcURL toURL:dstURL proposedURL:&propURL];
                    
                    [propURL retain];
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
                [propURL release];
                
                if (strcmp(new_dst, dst) != 0 && strcmp(new_dst, *prop_dst) != 0)
                {
                    free(*prop_dst);
                    asprintf(prop_dst, "%s", new_dst);
                }
            }
        }
        else
        {
            if (status->overwrite)
                result = NTFileConflictResolutionReplace;
            else
                result = NTFileConflictResolutionQuit;
        }
    }
    else
    {
        if (status->overwrite)
            result = NTFileConflictResolutionReplace;
        else
        {
            char *errpath = malloc(sizeof(char) * strlen(status->errpath) + 1);
            strcpy(errpath, status->errpath);
            status->errpath = errpath;
            
            status->err = EEXIST;
            result = NTFileConflictResolutionQuit;
        }
    }

    return result;
}

BOOL delegate_error(NTFileOperationStage stage, const char *src, const char *dst, void *ctx)
{
    __block BOOL result;
    
    op_status *status = (op_status *)ctx;
    status->err = errno;
    
    if (status->err == EACCES && status->skip_perm_err)
        result = YES;
    else if (status->async)
    {
        NTFileOperation *operation = (NTFileOperation *)status->operation;
        SEL delegateMethod = NULL;
        
        if (stage == NTFileOperationStageRunning)
        {
            if (status->usrcopy)
                delegateMethod = @selector(fileOperation:shouldProceedAfterError:copyingItemAtURL:toURL:);
            else if (status->delete)
                delegateMethod = @selector(fileOperation:shouldProceedAfterError:deletingItemAtURL:);
            else
                delegateMethod = @selector(fileOperation:shouldProceedAfterError:movingItemAtURL:toURL:);
        }
        else
            delegateMethod = @selector(fileOperation:shouldProceedAfterError:preflightingItemAtURL:toURL:);
        
        if ([[operation delegate] respondsToSelector:delegateMethod])
        {
            NSURL *dstURL;
            NSURL *srcURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:src length:strlen(src)]];
            NSURL *failureURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:status->errpath length:strlen(status->errpath)]];
            
            if (dst)
                dstURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:dst length:strlen(dst)]];
            else
                dstURL = nil;
            
            NSMutableDictionary *userInfo = [NSMutableDictionary dictionaryWithObjectsAndKeys:
                                             [NSString stringWithUTF8String:strerror(status->err)], NSLocalizedDescriptionKey,
                                             failureURL, NTFileOperationErrorURLKey,
                                             [NSNumber numberWithInt:status->errreason], NTFileOperationErrorReasonKey,
                                             // something for localized version, NSLocalizedFailureReasonErrorKey,
                                             [NSArray arrayWithObjects:@"Skip", @"Quit", nil], NSLocalizedRecoveryOptionsErrorKey, nil];
            
            if (status->err == ERANGE)
                [userInfo setObject:[NSNumber numberWithUnsignedLongLong:status->missing_bytes] forKey:NTFileOperationErrorNeddedSpaceKey];
            
            NSError *error = [NSError errorWithDomain:NSPOSIXErrorDomain code:status->err userInfo:[NSDictionary dictionaryWithDictionary:userInfo]];
            
            dispatch_group_t group =  dispatch_group_create();
            
            dispatch_group_async(group, status->queue, ^
            {
                @try
                {
                    if (stage == NTFileOperationStageRunning)
                    {
                        if (status->usrcopy)
                            result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error copyingItemAtURL:srcURL toURL:dstURL];
                        else if (status->delete)
                            result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error deletingItemAtURL:srcURL];
                        else
                            result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error movingItemAtURL:srcURL toURL:dstURL];
                    }
                    else
                        result = [[operation delegate] fileOperation:operation shouldProceedAfterError:error preflightingItemAtURL:srcURL toURL:dstURL];
                }
                @catch (NSException *e)
                {
                    NSLog(@"-[%@ %@] exception (calling queue): %@", NSStringFromClass([operation class]), NSStringFromSelector(delegateMethod), e);
                }
            });
            
            dispatch_group_wait(group, DISPATCH_TIME_FOREVER);
            dispatch_release(group);
        }
        else
            result = NO;
    }
    else
    {
        char *errpath = malloc(sizeof(char) * strlen(status->errpath) + 1);
        strcpy(errpath, status->errpath);
        status->errpath = errpath;
        
        result = NO;
    }
    
    return result;
}

BOOL delegate_progress(NTFileOperationStage stage, const char *src, const char *dst, void *ctx)
{
    __block BOOL result = YES;
    op_status *status = (op_status *)ctx;
    
    if (status->async && status->enabled)
    {
        NTFileOperation *operation = (NTFileOperation *)status->operation;
        SEL delegateMethod = @selector(fileOperation:shouldProceedOnProgressInfo:);
        
        if ([[operation delegate] respondsToSelector:delegateMethod])
        {
            NTFileOperationType type;
            
            if (status->usrcopy)
                type = NTFileOperationTypeCopy;
            else if (status->delete)
                type = NTFileOperationTypeDelete;
            else
                type = NTFileOperationTypeMove;
            
            NSMutableDictionary *info = [NSMutableDictionary dictionaryWithObjectsAndKeys:
                                         [NSNumber numberWithUnsignedInteger:type], NTFileOperationTypeKey,
                                         [NSNumber numberWithUnsignedInteger:stage], NTFileOperationStageKey,
                                         [[NSFileManager defaultManager] stringWithFileSystemRepresentation:status->src length:strlen(status->src)], NTFileOperationSourcePathKey,
                                         [[NSFileManager defaultManager] stringWithFileSystemRepresentation:src length:strlen(src)], NTFileOperationSourceItemPathKey,
                                         [NSNumber numberWithUnsignedLongLong:status->total_objects], NTFileOperationTotalObjectsKey, nil];
            
            if (dst)
                [info setObject:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:dst length:strlen(dst)] forKey:NTFileOperationDestinationItemPathKey];
            
            if (status->dst)
                [info setObject:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:status->dst length:strlen(status->dst)] forKey:NTFileOperationDestinationPathKey];
            
            if (stage == NTFileOperationStageRunning)
            {
                uint64_t completed;
                
                if (type == NTFileOperationTypeDelete)
                    completed = status->completed_objects;
                else
                    completed = status->completed_bytes;
                    
                for (int i = 0; i < 9; i++)
                    status->throughputs[i] = status->throughputs[i+1];
                
                struct timeval currtimeval;
                gettimeofday(&currtimeval, NULL);
                uint64_t curr_time = currtimeval.tv_sec * 1000000 + currtimeval.tv_usec;
                
                status->throughputs[9] = (completed - status->start_bytes) * 1000000 / (curr_time - status->start_time);
                
                uint64_t throughput = status->throughputs[9];
                
                for (int i = 0; i < 9; i++)
                    throughput += status->throughputs[i];
                
                throughput /= 10;
                
                status->start_bytes = completed;
                status->start_time = curr_time;
                
                [info setObject:[NSNumber numberWithUnsignedLongLong:status->completed_objects] forKey:NTFileOperationCompletedObjectsKey];
                [info setObject:[NSNumber numberWithUnsignedLongLong:throughput] forKey:NTFileOperationThroughputKey];
                
                if (type != NTFileOperationTypeDelete)
                {
                    [info setObject:[NSNumber numberWithUnsignedLongLong:status->total_bytes] forKey:NTFileOperationTotalBytesKey];
                    [info setObject:[NSNumber numberWithUnsignedLongLong:status->completed_bytes] forKey:NTFileOperationCompletedBytesKey];
                }
            }
            
            dispatch_group_t group =  dispatch_group_create();
            
            dispatch_group_async(group, status->queue, ^
            {
                @try
                {
                    result = [[operation delegate] fileOperation:operation shouldProceedOnProgressInfo:[NSDictionary dictionaryWithDictionary:info]];
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

int delete(char * const *paths, int flags, void *ctx)
{
    int result = 0;
    
    FTS *tree = fts_open(paths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
    
    if (tree)
    {
        char *prefix = malloc(sizeof(char) * 3);
        FTSENT *node;
        
        while ((result == 0) && (node = fts_read(tree)))
        {
            if ((flags & O_EXCL) && node->fts_level > 0)
                break;
            
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

#define UPDATE_PREFLIGHT_INFO { status->total_objects++;\
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

int preflight_copymove(char * const *srcs, char * const *dsts, void *ctx)
{
    int result = 0;
    int i = 0;
    
    char *src;
    char *dst;
    const char *dstdir;
    
    struct stat srcst;
    struct stat dstst;
    struct stat dstdirst;
    BOOL supportsBigFiles;
    
    struct statfs stfs;
    struct attrlist attrlst;
    
    op_status *status = (op_status *)ctx;
    
    while (result == 0 && srcs[i] != 0)
    {
        src = srcs[i];
        dst = dsts[i++];
        dstdir = dirname(dst);
        
        status->errpath = src;
        status->errreason = NTFileErrorReasonOther;
        
        if ((result = lstat(src, &srcst)) == 0)
        {
            status->errpath = (char *)dstdir;
            
            if ((result = lstat(dstdir, &dstdirst)) == 0)
            {
                if (!S_ISDIR(dstdirst.st_mode))
                {
                    result = -1;
                    errno = ENOTDIR;
                }
                
                if (result == 0 && srcst.st_dev != dstdirst.st_dev)
                {
                    if (status->move_acr_vol)
                        status->copy = YES;
                    else
                    {
                        result = -1;
                        errno = EXDEV;
                    }
                }
            }
        }
        
        if (result != 0)
        {
            if (delegate_error(NTFileOperationStagePreflighting, src, dst, ctx))
                result = 0;
            else
            {
                result = -1;
                errno = ECANCELED;
            }
        }
    }
    
    i = 0;
    while (srcs[i++] != 0);
    
    dev_t devs[i];
    uint64_t devtotalbytes[i];
    uint64_t dsttotalbytes[i];
    uint64_t devreplacebytes[i];
    uint64_t dstreplacebytes[i];
    uint64_t devfreebytes[i];
    
    i = 0;
    while (result == 0 && srcs[i] != 0)
    {
        devs[i] = 0;
        devreplacebytes[i] = 0;
        devtotalbytes[i] = 0;
        devfreebytes[i] = 0;
        dstreplacebytes[i] = 0;
        dsttotalbytes[i] = status->total_bytes;
        
        status->errreason = NTFileErrorReasonOther;
        
        src = srcs[i];
        dst = dsts[i];
        dstdir = dirname(dst);
        const char *dstname = basename(dst);
        
        status->src = src;
        status->dst = dst;
        
        status->errpath = (char *)dstdir;
        
        if (result == 0)
        {
            result = access(dstdir, _WRITE_OK | _APPEND_OK);
            
            if (result != 0)
                status->errreason = NTFileErrorReasonItemNotWritable;
        }
        
        if (result == 0)
        {
            supportsBigFiles = YES;
            
            long max_fsizebits = pathconf(dstdir, _PC_FILESIZEBITS);
            
            if (max_fsizebits == -1)
            {
                if (statfs(dstdir, &stfs) == 0)
                {
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
            
            char *dstpath;
            asprintf(&dstpath, "%s", dstdir);
            
            char pathsep = '/';
            char * const srcpaths[] = { src, 0 };
            
            status->errpath = src;
            
            FTSENT *node;
            FTS *tree = fts_open(srcpaths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
            
            if (tree)
            {
                short fts_level = -1;
                short replace_level = SHRT_MAX;
                short keepboth_level = SHRT_MAX;
                
                char *prefix = malloc(sizeof(char) * 3);
                
                while ((result == 0) && (node = fts_read(tree)))
                {
                    status->errreason = NTFileErrorReasonOther;
                    
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
                        
                        char *temppath;
                        asprintf(&temppath, "%s", dstpath);
                        
                        free(dstpath);
                        
                        if (node->fts_level > fts_level)
                            asprintf(&dstpath, "%s%c%s", temppath, pathsep, (node->fts_level == 0) ? dstname : node->fts_name);
                        else if (node->fts_level < fts_level)
                            asprintf(&dstpath, "%s", dirname(temppath));
                        else
                        {
                            char *dirpath;
                            
                            asprintf(&dirpath, "%s", dirname(temppath));
                            asprintf(&dstpath, "%s%c%s", dirpath, pathsep, (node->fts_level == 0) ? dstname : node->fts_name);
                            
                            free(dirpath);
                        }
                        
                        free(temppath);
                        
                        fts_level = node->fts_level;
                        
                        if (fts_level <= replace_level)
                            replace_level = SHRT_MAX;
                        
                        if (fts_level <= keepboth_level)
                            keepboth_level = SHRT_MAX;
                        
                        if (node->fts_info != FTS_DP)
                        {
                            char *dstdirpath;
                            char *srcdirpath;
                            
                            asprintf(&dstdirpath, "%s%c", dst, pathsep);
                            asprintf(&srcdirpath, "%s%c", node->fts_path, pathsep);
                            
                            status->errpath = dstpath;
                            
                            if (strstr(dstdirpath, srcdirpath) != NULL && strlen(dstdirpath) > strlen(srcdirpath))
                            {
                                result = -1;
                                errno = EINVAL;
                            }
                            
                            free(dstdirpath);
                            free(srcdirpath);
                            
                            if (!status->copy && strcmp(node->fts_path, dstpath) == 0)
                            {
                                result = -1;
                                errno = EINVAL;
                            }
                        }
                        
                        status->errpath = node->fts_path;
                        
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
                                status->errpath = dstpath;
                                
                                if (replace_level == SHRT_MAX && keepboth_level == SHRT_MAX && lstat(dstpath, &dstst) == 0)
                                {
                                    // Dest file exists!!!
                                    // Create proposed new destination name.
                                    char *extension = NULL;
                                    char *dot = strrchr(node->fts_name, '.');
                                    
                                    if (dot && strlen(dot) > 1 && strlen(node->fts_name) > strlen(dot))
                                        extension = dot + 1;
                                    
                                    size_t noextlen;
                                    
                                    if (extension)
                                        noextlen = strlen(dstpath) - strlen(extension) - 1;
                                    else
                                        noextlen = strlen(dstpath);
                                    
                                    char *new_dst_noext = malloc(sizeof(char) * (noextlen + 1));
                                    strncpy(new_dst_noext, dstpath, noextlen);
                                    new_dst_noext[noextlen] = '\0';
                                    
                                    char *new_dst;
                                    
                                    if (extension)
                                        asprintf(&new_dst, "%s%s.%s", new_dst_noext, " added", extension);
                                    else
                                        asprintf(&new_dst, "%s%s", new_dst_noext, " added");
                                    
                                    free(new_dst_noext);
                                    
                                    NTFileConflictResolution resolution = delegate_conflict(node->fts_path, dstpath, &new_dst, ctx);
                                    
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
                                            if (strcmp(node->fts_path, dstpath) == 0)
                                            {
                                                result = -1;
                                                errno = EINVAL;
                                            }
                                            
                                            if (result == 0)
                                            {
                                                status->errpath = node->fts_path;
                                                
                                                if (!status->copy)
                                                {
                                                    fts_set(tree, node, FTS_SKIP);
                                                    result = access_move(node->fts_path);
                                                    
                                                    if (result != 0)
                                                    {
                                                        if (errno == EPERM)
                                                            status->errreason = NTFileErrorReasonItemLocked;
                                                        else if (errno == EACCES)
                                                            status->errreason = NTFileErrorReasonItemNotMovable;
                                                    }
                                                }
                                                else
                                                {
                                                    result = access_read(node->fts_path);
                                                    
                                                    if (result != 0 && errno == EACCES)
                                                        status->errreason = NTFileErrorReasonItemNotReadable;
                                                }
                                                
                                                if (result == 0)
                                                {
                                                    status->errpath = dstpath;
                                                    off_t size = 0;
                                                    
                                                    result = access_delete(dstpath, &size);
                                                    
                                                    if (result != 0)
                                                    {
                                                        if (errno == EPERM)
                                                            status->errreason = NTFileErrorReasonItemLocked;
                                                        else if (errno == EACCES)
                                                            status->errreason = NTFileErrorReasonItemNotDeletable;
                                                    }
                                                    else
                                                        dstreplacebytes[i] += size;
                                                }
                                                
                                                if (result == 0)
                                                {
                                                    if (status->copy)
                                                        UPDATE_PREFLIGHT_INFO
                                                        
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
                                            status->errpath = node->fts_path;
                                            
                                            if (!status->copy)
                                            {
                                                fts_set(tree, node, FTS_SKIP);
                                                result = access_move(node->fts_path);
                                                
                                                if (result != 0)
                                                {
                                                    if (errno == EPERM)
                                                        status->errreason = NTFileErrorReasonItemLocked;
                                                    else if (errno == EACCES)
                                                        status->errreason = NTFileErrorReasonItemNotMovable;
                                                }
                                            }
                                            else
                                            {
                                                result = access_read(node->fts_path);
                                                
                                                if (result != 0 && errno == EACCES)
                                                    status->errreason = NTFileErrorReasonItemNotReadable;
                                            }
                                            
                                            if ((result == 0) && (node->fts_info == FTS_D))
                                            {
                                                status->errpath = dirname(dstpath);
                                                result = access(dirname(dstpath), _WRITE_OK | _APPEND_OK);
                                                
                                                if (result != 0 && errno == EACCES)
                                                    status->errreason = NTFileErrorReasonItemNotWritable;
                                            }
                                            
                                            if (result == 0)
                                            {
                                                if (status->copy)
                                                    UPDATE_PREFLIGHT_INFO
                                                    
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
                                            status->errpath = node->fts_path;
                                            
                                            if (status->copy)
                                            {
                                                result = access_read(node->fts_path);
                                                
                                                if (result != 0 && errno == EACCES)
                                                    status->errreason = NTFileErrorReasonItemNotReadable;
                                            }
                                            else
                                            {
                                                result = access_delete(node->fts_path, NULL);
                                                
                                                if (result != 0)
                                                {
                                                    if (errno == EPERM)
                                                        status->errreason = NTFileErrorReasonItemLocked;
                                                    else if (errno == EACCES)
                                                        status->errreason = NTFileErrorReasonItemNotDeletable;
                                                }
                                            }
                                            
                                            if ((result == 0) && (node->fts_info == FTS_D))
                                            {
                                                status->errpath = dstpath;
                                                result = access(dstpath, _WRITE_OK | _APPEND_OK);
                                                
                                                if (result != 0 && errno == EACCES)
                                                    status->errreason = NTFileErrorReasonItemNotWritable;
                                            }
                                            
                                            if (status->copy && result == 0)
                                                UPDATE_PREFLIGHT_INFO
                                                
                                            break;
                                        }
                                    }
                                    
                                    free(new_dst);
                                }
                                else if (replace_level != SHRT_MAX || keepboth_level != SHRT_MAX || errno == ENOENT)
                                {
                                    // Dest file doesn't exist!!!
                                    status->errpath = node->fts_path;
                                    
                                    if (!status->copy)
                                    {
                                        fts_set(tree, node, FTS_SKIP);
                                        result = access_move(node->fts_path);
                                        
                                        if (result != 0)
                                        {
                                            if (errno == EPERM)
                                                status->errreason = NTFileErrorReasonItemLocked;
                                            else if (errno == EACCES)
                                                status->errreason = NTFileErrorReasonItemNotMovable;
                                        }
                                    }
                                    else
                                    {
                                        result = access_read(node->fts_path);
                                        
                                        if (result != 0 && errno == EACCES)
                                            status->errreason = NTFileErrorReasonItemNotReadable;
                                    }
                                    
                                    if (status->copy && result == 0)
                                        UPDATE_PREFLIGHT_INFO
                                }
                                else
                                    result = -1;
                            }
                        }
                    }
                    else if ((node->fts_info == FTS_DNR) || (node->fts_info == FTS_ERR) || (node->fts_info == FTS_NS))
                    {
                        status->errpath = node->fts_path;
                        result = -1;
                    }
                    else if ((node->fts_info == FTS_DC) || (node->fts_info == FTS_INIT) || (node->fts_info == FTS_NSOK) || (node->fts_info == FTS_W))
                    {
                        status->errpath = node->fts_path;
                        result = -1;
                        errno = EBADF;
                    }
                    
                    if (result == 0)
                    {
                        if (!delegate_progress(NTFileOperationStagePreflighting, node->fts_path, dstpath, ctx))
                        {
                            result = -1;
                            errno = ECANCELED;
                        }
                    }
                    else if (errno != ECANCELED)
                    {
                        if (delegate_error(NTFileOperationStagePreflighting, node->fts_path, dstpath, ctx))
                        {
                            result = 0;
                            
                            fts_set(tree, node, FTS_SKIP);
                            SKIP_FILE_PATH(node->fts_path)
                        }
                        else
                            errno = ECANCELED;
                    }
                }
                
                free(prefix);
                fts_close(tree);
                
                dsttotalbytes[i] = status->total_bytes - dsttotalbytes[i];
            }
            
            if (result == 0 && errno != 0)
            {
                // This happens if fts_read() or fts_open() fails.
                if (delegate_error(NTFileOperationStagePreflighting, src, dstpath, ctx))
                {
                    result = 0;
                    SKIP_FILE_PATH(src)
                }
                else
                {
                    result = -1;
                    errno = ECANCELED;
                }
            }
            
            free(dstpath);
        }
        else
        {
            if (delegate_error(NTFileOperationStagePreflighting, src, dst, ctx))
            {
                result = 0;
                SKIP_FILE_PATH(src)
            }
            else
            {
                result = -1;
                errno = ECANCELED;
            }
        }
        
        i++;
    }
    
    devs[i] = 0;
    
    if (result == 0 && status->copy)
    {
        char **devsrcpaths = NULL;
        char **devdstpaths = NULL;
        long devsrcpos = 0;
        long devdstpos = 0;
        
        i = 0;
        int j;
        void *attrbuf = malloc(32);
        
        while (srcs[i] != 0)
        {
            src = srcs[i];
            dst = dsts[i];
            dstdir = dirname(dst);
            
            if (lstat(dstdir, &dstdirst) == 0)
            {
                BOOL found = NO;
                
                dev_t dev = dstdirst.st_dev;
                
                for (j = 0; devs[j] != 0; j++)
                {
                    if (devs[j] == dev)
                    {
                        devtotalbytes[j] += dsttotalbytes[i];
                        devreplacebytes[j] += dstreplacebytes[i];
                        found = YES;
                        break;
                    }
                    
                }
                
                if (!found)
                {
                    devs[j] = dev;
                    devtotalbytes[j] += dsttotalbytes[i];
                    devreplacebytes[j] += dstreplacebytes[i];
                    
                    size_t len;
                    char *devpath;
                    
                    len = devsrcpos + strlen(src) + 1;
                    devsrcpaths = realloc(devsrcpaths, sizeof(char *) * len);
                    devpath = (char *)devsrcpaths + devsrcpos;
                    strcpy(devpath, src);
                    devsrcpos = len;
                    
                    len = devdstpos + strlen(dst) + 1;
                    devdstpaths = realloc(devdstpaths, sizeof(char *) * len);
                    devpath = (char *)devdstpaths + devdstpos;
                    strcpy(devpath, dst);
                    devdstpos = len;
                    
                    if (statfs(dstdir, &stfs) == 0)
                    {
                        attrlst.bitmapcount = ATTR_BIT_MAP_COUNT;
                        attrlst.reserved = 0;
                        attrlst.commonattr = ATTR_CMN_RETURNED_ATTRS;
                        attrlst.volattr = ATTR_VOL_INFO | ATTR_VOL_SPACEAVAIL;
                        attrlst.dirattr = 0;
                        attrlst.fileattr = 0;
                        attrlst.forkattr = 0;
                        
                        if (getattrlist(stfs.f_mntonname, &attrlst, attrbuf, 32, FSOPT_NOFOLLOW ) == 0)
                            devfreebytes[j] = *((u_int64_t *)attrbuf + 3);
                    }
                }
            }
            
            i++;
        }
        
        free(attrbuf);
        
        i = 0;
        devsrcpos = 0;
        devdstpos = 0;
        
        while (result == 0 && devs[i] != 0)
        {
            char *devsrcpath = (char *)devsrcpaths + devsrcpos;
            char *devdstpath = (char *)devdstpaths + devdstpos;
            
            if (devtotalbytes[i] > devreplacebytes[i])
            {
                if (devfreebytes[i] > 0 && (devtotalbytes[i] - devreplacebytes[i]) > devfreebytes[i])
                {
                    status->missing_bytes = devtotalbytes[i] - devreplacebytes[i] - devfreebytes[i];
                    status->errpath = dirname(devdstpath);
                    status->errreason = NTFileErrorReasonNoFreeSpace;
                    errno = ERANGE;
                    
                    if (delegate_error(NTFileOperationStagePreflighting, devsrcpath, devdstpath, ctx))
                        result = 0;
                    else
                    {
                        result = -1;
                        errno = ECANCELED;
                    }
                }
                
            }
            
            devsrcpos += strlen(devsrcpath) + 1;
            devdstpos += strlen(devdstpath) + 1;
            
            i++;
        }
        
        free(devsrcpaths);
        free(devdstpaths);
    }
    
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

int preflight_delete(char * const * srcs, void *ctx)
{
    int result = 0;
    
    op_status *status = (op_status *)ctx;
    status->dst = NULL;
    
    char *src;
    asprintf(&src, "%s", srcs[0]);
    
    FTSENT *node;
    FTS *tree = fts_open(srcs, FTS_PHYSICAL | FTS_NOCHDIR, 0);
    
    if (tree)
    {
        char *prefix = malloc(sizeof(char) * 3);
        
        while ((result == 0) && (node = fts_read(tree)))
        {
            if (node->fts_level == 0)
                status->src = node->fts_path;
            
            status->errpath = node->fts_path;
            status->errreason = NTFileErrorReasonOther;
            
            free(src);
            asprintf(&src, "%s", node->fts_path);
            
            if (node->fts_info == FTS_F || node->fts_info == FTS_DP || node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE || node->fts_info == FTS_DEFAULT)
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
                
                if (node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE)
                    result = access_delete(node->fts_path, NULL);
                else
                    result = access(node->fts_path, _DELETE_OK);
                
                if (result != 0 && errno == EACCES)
                    status->errreason = NTFileErrorReasonItemNotDeletable;
                
                if (result == 0)
                    status->total_objects++;
                    
            }
            else if ((node->fts_info == FTS_DNR) || (node->fts_info == FTS_ERR) || (node->fts_info == FTS_NS))
                result = -1;
            else if ((node->fts_info == FTS_DC) || (node->fts_info == FTS_INIT) || (node->fts_info == FTS_NSOK) || (node->fts_info == FTS_W))
            {
                result = -1;
                errno = EBADF;
            }
            
            if (result == 0 && !delegate_progress(NTFileOperationStagePreflighting, node->fts_path, NULL, ctx))
            {
                result = -1;
                errno = ECANCELED;
            }
            
            if (result != 0 && errno != ECANCELED && delegate_error(NTFileOperationStagePreflighting, node->fts_path, NULL, ctx))
            {
                result = 0;
                SKIP_FILE_PATH(node->fts_path)
            }
        }
        
        free(prefix);
        fts_close(tree);
    }
    
    if (result == 0 && errno != 0)
    {
        // This happens if fts_read() or fts_open() fails.
        if (delegate_error(NTFileOperationStagePreflighting, src, NULL, ctx))
        {
            result = 0;
            SKIP_FILE_PATH(src)
        }
        else
        {
            result = -1;
            errno = ECANCELED;
        }
    }
    
    free(src);
    
    char *path;
    
    status->skip_paths = realloc(status->skip_paths, sizeof(char *) * (status->skip_pos + 1));
    path = (char *)status->skip_paths + status->skip_pos;
    path[0] = '\0';
    
    return result;
}

int progress_copymove(int what, int stage, copyfile_state_t state, const char *src, const char *dst, void *ctx)
{
    int result = COPYFILE_CONTINUE;
    
    op_status *status = (op_status *)ctx;
    
    if (stage == COPYFILE_START)
    {
        if (what == COPYFILE_RECURSE_FILE)
        {
            char *prefix = malloc(sizeof(char) * 3);
            
            strncpy(prefix, basename((char *)src), 2);
            prefix[2] = '\0';
            
            if (strcmp(prefix, "._") == 0)
                result = COPYFILE_SKIP;
            
            free(prefix);
        }
        
        if (result == COPYFILE_CONTINUE && (what == COPYFILE_RECURSE_DIR || what == COPYFILE_RECURSE_FILE))
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
                        
                        if (delete(paths, 0, ctx) != 0)
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
                            copyfile_state_t newstate = copyfile_state_alloc();
                            
                            copyfile_state_set(newstate, COPYFILE_STATE_STATUS_CB, callback);
                            copyfile_state_set(newstate, COPYFILE_STATE_STATUS_CTX, ctx);
                            copyfile_flags_t flags = COPYFILE_ALL | COPYFILE_RECURSIVE | COPYFILE_NOFOLLOW;
                            
                            copyfile(src, new_dst, newstate, flags);
                            copyfile_state_free(newstate);
                            
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
            {
                status->dir_responses = realloc(status->dir_responses, sizeof(int) * (status->dir_resp_cnt + 1));
                *(status->dir_responses + status->dir_resp_cnt++) = result;
            }
            
            if (what == COPYFILE_RECURSE_FILE)
                status->current_bytes = 0;
        }
        else if (result == COPYFILE_CONTINUE && what == COPYFILE_RECURSE_DIR_CLEANUP)
            result = *(status->dir_responses + --status->dir_resp_cnt);
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
            status->completed_objects++;
            
            if (what == COPYFILE_RECURSE_DIR_CLEANUP && !status->copy)
            {
                // Delete (silently) only of dir is empty, no skipped files.
                char *paths[] = { (char *)src, 0 };
                delete(paths, O_EXCL, ctx);
            }
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
            status->errpath = (char *)src;
            status->errreason = NTFileErrorReasonOther;
            
            if (delegate_error(NTFileOperationStageRunning, src, dst, ctx))
                result = COPYFILE_SKIP;
            else
                result = COPYFILE_QUIT;
        }
        else
            result = COPYFILE_QUIT;
    }
    
    return result;
}

int deletefiles(char * const *paths, void *ctx)
{
    int result = 0;
    
    op_status *status = (op_status *)ctx;
    status->dst = NULL;
    
    char *src;
    asprintf(&src, "%s", paths[0]);
    
    FTS *tree = fts_open(paths, FTS_PHYSICAL | FTS_NOCHDIR, 0);
    
    if (tree)
    {
        char *prefix = malloc(sizeof(char) * 3);
        FTSENT *node;
        
        while ((result == 0) && (node = fts_read(tree)))
        {
            if (node->fts_level == 0)
                status->src = node->fts_path;
            
            status->errpath = node->fts_path;
            status->errreason = NTFileErrorReasonOther;
            
            free(src);
            asprintf(&src, "%s", node->fts_path);
            
            if (node->fts_info == FTS_F || node->fts_info == FTS_DP || node->fts_info == FTS_SL || node->fts_info == FTS_SLNONE || node->fts_info == FTS_DEFAULT)
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
                
                char *res_path = (char *)status->skip_paths + status->skip_pos;
                
                if (res_path[0] != '\0' && strcmp(node->fts_path, res_path) == 0)
                {
                    // SKIPED FILE!!!
                    status->skip_pos += strlen(res_path) + 1;
                    continue;
                }
                
                if (node->fts_info != FTS_DP)
                    result = unlink(node->fts_path);
                else
                {
                    result = rmdir(node->fts_path);
                    
                    if (result != 0 && errno == ENOTEMPTY)
                        result = 0;
                }
                
                status->completed_objects++;
            }
            else if ((node->fts_info == FTS_DNR) || (node->fts_info == FTS_ERR) || (node->fts_info == FTS_NS))
                result = -1;
            else if ((node->fts_info == FTS_DC) || (node->fts_info == FTS_INIT) || (node->fts_info == FTS_NSOK) || (node->fts_info == FTS_W))
            {
                result = -1;
                errno = EBADF;
            }
            
            if (result == 0 && !delegate_progress(NTFileOperationStageRunning, node->fts_path, NULL, ctx))
            {
                result = -1;
                errno = ECANCELED;
            }
            
            if (result != 0 && errno != ECANCELED && delegate_error(NTFileOperationStageRunning, node->fts_path, NULL, ctx))
            {
                result = 0;
                SKIP_FILE_PATH(node->fts_path)
            }
        }
        
        free(prefix);
        fts_close(tree);
    }

    if (result == 0 && errno != 0)
    {
        // This happens if fts_read() or fts_open() fails.
        if (delegate_error(NTFileOperationStagePreflighting, src, NULL, ctx))
        {
            result = 0;
            SKIP_FILE_PATH(src)
        }
        else
        {
            result = -1;
            errno = ECANCELED;
        }
    }
    
    free(src);
    
    return result;
}

@interface NTFileOperation (Private)

- (void)checkSources:(NSArray *)theSrcURLs destinations:(NSArray *)aDstURL;
- (void)doAsyncCopy:(BOOL)isCopy itemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)aDstURL options:(NSUInteger)anOptMask;
- (BOOL)doSyncCopy:(BOOL)isCopy itemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)aDstURL options:(NSUInteger)anOptMask error:(NSError **)anError;

@end

@implementation NTFileOperation

- (void)dealloc
{
    [self setDelegate:nil];
    
    [super dealloc];
}

- (id)init
{
    self = [super init];
    
    [self setStatusChangeInterval:0.4];
    
    return self;
}

- (void)copyAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask
{
    [self copyAsyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURLs:[NSArray arrayWithObject:aDstURL] options:anOptMask];
}

- (void)copyAsyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask
{
    [self doAsyncCopy:YES itemsAtURLs:theSrcURLs toURLs:theDstURLs options:anOptMask];
}

- (BOOL)copySyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask error:(NSError **)anError
{
    return [self copySyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURLs:[NSArray arrayWithObject:aDstURL] options:anOptMask error:anError];
}

- (BOOL)copySyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask error:(NSError **)anError
{
    return [self doSyncCopy:YES itemsAtURLs:theSrcURLs toURLs:theDstURLs options:anOptMask error:anError];
}

- (void)moveAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask
{
    [self moveAsyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURLs:[NSArray arrayWithObject:aDstURL] options:anOptMask];
}

- (void)moveAsyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURL options:(NSUInteger)anOptMask
{
    [self doAsyncCopy:NO itemsAtURLs:theSrcURLs toURLs:theDstURL options:anOptMask];
}

- (BOOL)moveSyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask error:(NSError **)anError
{
    return [self moveSyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] toURLs:[NSArray arrayWithObject:aDstURL] options:anOptMask error:anError];
}

- (BOOL)moveSyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask error:(NSError **)anError
{
    return [self doSyncCopy:NO itemsAtURLs:theSrcURLs toURLs:theDstURLs options:anOptMask error:anError];
}

- (void)deleteAsyncItemAtURL:(NSURL *)aSrcURL options:(NSUInteger)anOptMask
{
    [self deleteAsyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] options:anOptMask];
}

- (void)deleteAsyncItemsAtURLs:(NSArray *)theSrcURLs options:(NSUInteger)anOptMask
{
    [self checkSources:theSrcURLs destinations:nil];
    
    if ([theSrcURLs count] == 0)
        return;
    
    dispatch_queue_t queue = dispatch_get_current_queue();
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^
    {
        @try
        {
            op_status status;
            
            status.operation = (void *)self;
            status.async = YES;
            status.copy = NO;
            status.usrcopy = NO;
            status.delete = YES;
            status.skip_perm_err = (anOptMask & NTFileOperationSkipPermissionErrors) == NTFileOperationSkipPermissionErrors;
            status.queue = queue;
            status.timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0));
            status.interval = [self statusChangeInterval];
            status.enabled = YES;
            status.total_bytes = 0;
            status.total_objects = 0;
            status.dir_responses = NULL;
            status.skip_paths = NULL;
            status.skip_pos = 0;
            status.replace_paths = NULL;
            status.replace_pos = 0;
            status.keepboth_src_paths = NULL;
            status.keepboth_src_pos = 0;
            status.keepboth_dst_paths = NULL;
            status.keepboth_dst_pos = 0;
            status.errcnt = 0;
            
            int i = 0;
            char *srcs[[theSrcURLs count] + 1];
            
            for (NSURL *srcURL in theSrcURLs)
                srcs[i++] = (char *)[[srcURL path] fileSystemRepresentation];
            srcs[i] = 0;
            
            dispatch_resume(status.timer);
            
            if (preflight_delete(srcs, (void *)&status) == 0)
            {
                status.enabled = YES;
                status.completed_bytes = 0;
                status.completed_objects = 0;
                status.current_bytes = 0;
                status.start_bytes = 0;
                status.start_time = 0;
                status.dir_resp_cnt = 0;
                status.skip_pos = 0;
                
                for (i = 0; i < 10; i++)
                    status.throughputs[i] = 0;
                
                deletefiles(srcs, (void *)&status);
                
                dispatch_release(status.timer);
            }
            
            free(status.dir_responses);
            free(status.skip_paths);
            free(status.replace_paths);
            free(status.keepboth_src_paths);
            free(status.keepboth_dst_paths);
            
            SEL delegateMethod = @selector(fileOperation:shouldProceedOnProgressInfo:);
            
            if ([[self delegate] respondsToSelector:delegateMethod])
            {
                NSDictionary *info = [NSDictionary dictionaryWithObjectsAndKeys:
                                      [NSNumber numberWithUnsignedInteger:NTFileOperationTypeDelete], NTFileOperationTypeKey,
                                      [NSNumber numberWithUnsignedInteger:NTFileOperationStageComplete], NTFileOperationStageKey,
                                      [theSrcURLs lastObject], NTFileOperationSourcePathKey,
                                      [theSrcURLs lastObject], NTFileOperationSourceItemPathKey,
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

- (BOOL)deleteSyncItemAtURL:(NSURL *)aSrcURL options:(NSUInteger)anOptMask error:(NSError **)anError
{
    return [self deleteSyncItemsAtURLs:[NSArray arrayWithObject:aSrcURL] options:anOptMask error:anError];
}

- (BOOL)deleteSyncItemsAtURLs:(NSArray *)theSrcURLs options:(NSUInteger)anOptMask error:(NSError **)anError
{
    [self checkSources:theSrcURLs destinations:nil];
    
    if ([theSrcURLs count] == 0)
        return YES;
    
    op_status status;
    
    status.operation = (void *)self;
    status.async = NO;
    status.copy = NO;
    status.usrcopy = NO;
    status.delete = YES;
    status.skip_perm_err = (anOptMask & NTFileOperationSkipPermissionErrors) == NTFileOperationSkipPermissionErrors;
    status.total_bytes = 0;
    status.total_objects = 0;
    status.dir_responses = NULL;
    status.skip_paths = NULL;
    status.skip_pos = 0;
    status.replace_paths = NULL;
    status.replace_pos = 0;
    status.keepboth_src_paths = NULL;
    status.keepboth_src_pos = 0;
    status.keepboth_dst_paths = NULL;
    status.keepboth_dst_pos = 0;
    status.errcnt = 0;
    
    int i = 0;
    char *srcs[[theSrcURLs count] + 1];
    
    i = 0;
    for (NSURL *srcURL in theSrcURLs)
        srcs[i++] = (char *)[[srcURL path] fileSystemRepresentation];
    srcs[i] = 0;
    
    int result = preflight_delete(srcs, (void *)&status);
    
    if (result == 0)
    {
        status.completed_bytes = 0;
        status.completed_objects = 0;
        status.current_bytes = 0;
        status.start_bytes = 0;
        status.start_time = 0;
        status.dir_resp_cnt = 0;
        status.skip_pos = 0;
        
        result = deletefiles(srcs, (void *)&status);
    }
    
    free(status.dir_responses);
    free(status.skip_paths);
    free(status.replace_paths);
    free(status.keepboth_src_paths);
    free(status.keepboth_dst_paths);
    
    if (result == 0)
        return YES;
    else
    {
        if (anError)
        {
            NSURL *failureURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:status.errpath length:strlen(status.errpath)]];
            free(status.errpath);
            
            NSMutableDictionary *userInfo = [NSMutableDictionary dictionaryWithObjectsAndKeys:
                                             [NSString stringWithUTF8String:strerror(status.err)], NSLocalizedDescriptionKey,
                                             failureURL, NTFileOperationErrorURLKey,
                                             [NSNumber numberWithInt:status.errreason], NTFileOperationErrorReasonKey,
                                             // something for localized version, NSLocalizedFailureReasonErrorKey,
                                             [NSArray arrayWithObjects:@"Skip", @"Quit", nil], NSLocalizedRecoveryOptionsErrorKey, nil];
            
            if (status.err == ERANGE)
                [userInfo setObject:[NSNumber numberWithUnsignedLongLong:status.missing_bytes] forKey:NTFileOperationErrorNeddedSpaceKey];
            
            *anError = [NSError errorWithDomain:NSPOSIXErrorDomain code:status.err userInfo:[NSDictionary dictionaryWithDictionary:userInfo]];
        }
        
        return NO;
    }
}

@end

@implementation NTFileOperation (Private)

- (void)checkSources:(NSArray *)theSrcURLs destinations:(NSArray *)theDstURLs
{
    for (id source in theSrcURLs)
    {
        if (![source isKindOfClass:[NSURL class]])
            [NSException raise:NSInvalidArgumentException format:@"-[%@ %@] exception: source is not URL object.", NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
    
    if (theDstURLs)
    {
        for (id destination in theDstURLs)
        {
            if (![destination isKindOfClass:[NSURL class]])
                [NSException raise:NSInvalidArgumentException format:@"-[%@ %@] exception: destination is not URL object.", NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
        }
        
        if ([theSrcURLs count] != [theDstURLs count])
            [NSException raise:NSInvalidArgumentException format:@"-[%@ %@] exception: sources number doesn't match destinations number.", NSStringFromClass([self class]), NSStringFromSelector(_cmd)];
    }
}

- (void)doAsyncCopy:(BOOL)isCopy itemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask
{
    [self checkSources:theSrcURLs destinations:theDstURLs];
    
    if ([theSrcURLs count] == 0)
        return;
    
    dispatch_queue_t queue = dispatch_get_current_queue();
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^
    {
        @try
        {
            op_status status;
            
            status.operation = (void *)self;
            status.async = YES;
            status.copy = isCopy;
            status.usrcopy = isCopy;
            status.delete = NO;
            status.overwrite = (anOptMask & NTFileOperationOverwrite) == NTFileOperationOverwrite;
            status.skip_perm_err = (anOptMask & NTFileOperationSkipPermissionErrors) == NTFileOperationSkipPermissionErrors;
            status.move_acr_vol = !((anOptMask & NTFileOperationDoNotMoveAcrossVolumes) == NTFileOperationDoNotMoveAcrossVolumes);
            status.queue = queue;
            status.timer = dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0));
            status.interval = [self statusChangeInterval];
            status.enabled = YES;
            status.total_bytes = 0;
            status.total_objects = 0;
            status.dir_responses = NULL;
            status.skip_paths = NULL;
            status.skip_pos = 0;
            status.replace_paths = NULL;
            status.replace_pos = 0;
            status.keepboth_src_paths = NULL;
            status.keepboth_src_pos = 0;
            status.keepboth_dst_paths = NULL;
            status.keepboth_dst_pos = 0;
            status.errcnt = 0;
            
            int i;
            char *srcs[[theSrcURLs count] + 1];
            char *dsts[[theDstURLs count] + 1];
            
            i = 0;
            for (NSURL *srcURL in theSrcURLs)
                srcs[i++] = (char *)[[srcURL path] fileSystemRepresentation];
            srcs[i] = 0;
            
            i = 0;
            for (NSURL *dstURL in theDstURLs)
                dsts[i++] = (char *)[[dstURL path] fileSystemRepresentation];
            dsts[i] = 0;
            
            dispatch_resume(status.timer);
            
            if (preflight_copymove(srcs, dsts, (void *)&status) == 0)
            {
                status.enabled = YES;
                status.completed_bytes = 0;
                status.completed_objects = 0;
                status.current_bytes = 0;
                status.start_bytes = 0;
                status.start_time = 0;
                status.dir_resp_cnt = 0;
                status.skip_pos = 0;
                status.replace_pos = 0;
                status.keepboth_src_pos = 0;
                status.keepboth_dst_pos = 0;
                
                for (i = 0; i < 10; i++)
                    status.throughputs[i] = 0;
                
                copyfile_state_t state = copyfile_state_alloc();
                
                copyfile_state_set(state, COPYFILE_STATE_STATUS_CB, progress_copymove);
                copyfile_state_set(state, COPYFILE_STATE_STATUS_CTX, (void *)&status);
                copyfile_flags_t flags = COPYFILE_ALL | COPYFILE_RECURSIVE | COPYFILE_NOFOLLOW;
                
                int result;
                
                for (i = 0; i < [theSrcURLs count]; i++)
                {
                    char *src = srcs[i];
                    char *dst = dsts[i];
                    
                    status.src = src;
                    status.dst = dst;
                    
                    result = copyfile(src, dst, state, flags);
                    
                    if (result != 0)
                        break;
                }
                
                copyfile_state_free(state);
                
                if (status.copy && !status.usrcopy && result == 0 && status.skip_pos == 0 && status.errcnt == 0)
                    delete(srcs, 0, (void *)&status);
                
                dispatch_release(status.timer);
            }
            
            free(status.dir_responses);
            free(status.skip_paths);
            free(status.replace_paths);
            free(status.keepboth_src_paths);
            free(status.keepboth_dst_paths);
            
            SEL delegateMethod = @selector(fileOperation:shouldProceedOnProgressInfo:);
            
            if ([[self delegate] respondsToSelector:delegateMethod])
            {
                NTFileOperationType type = status.usrcopy ? NTFileOperationTypeCopy : NTFileOperationTypeMove;
                
                NSDictionary *info = [NSDictionary dictionaryWithObjectsAndKeys:
                                      [NSNumber numberWithUnsignedInteger:type], NTFileOperationTypeKey,
                                      [NSNumber numberWithUnsignedInteger:NTFileOperationStageComplete], NTFileOperationStageKey,
                                      [theSrcURLs lastObject], NTFileOperationSourcePathKey,
                                      [theSrcURLs lastObject], NTFileOperationSourceItemPathKey,
                                      [theDstURLs lastObject], NTFileOperationDestinationPathKey,
                                      [theDstURLs lastObject], NTFileOperationDestinationItemPathKey,
                                      [NSNumber numberWithUnsignedLongLong:status.total_objects], NTFileOperationTotalObjectsKey,
                                      [NSNumber numberWithUnsignedLongLong:status.completed_objects], NTFileOperationCompletedObjectsKey,
                                      [NSNumber numberWithUnsignedLongLong:0], NTFileOperationThroughputKey,
                                      [NSNumber numberWithUnsignedLongLong:status.total_bytes], NTFileOperationTotalBytesKey,
                                      [NSNumber numberWithUnsignedLongLong:status.completed_bytes], NTFileOperationCompletedBytesKey, nil];
                
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

- (BOOL)doSyncCopy:(BOOL)isCopy itemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask error:(NSError **)anError
{
    [self checkSources:theSrcURLs destinations:theDstURLs];
    
    if ([theSrcURLs count] == 0)
        return YES;
    
    op_status status;
    
    status.operation = (void *)self;
    status.async = NO;
    status.copy = isCopy;
    status.usrcopy = isCopy;
    status.delete = NO;
    status.overwrite = (anOptMask & NTFileOperationOverwrite) == NTFileOperationOverwrite;
    status.skip_perm_err = (anOptMask & NTFileOperationSkipPermissionErrors) == NTFileOperationSkipPermissionErrors;
    status.move_acr_vol = !((anOptMask & NTFileOperationDoNotMoveAcrossVolumes) == NTFileOperationDoNotMoveAcrossVolumes);
    status.total_bytes = 0;
    status.total_objects = 0;
    status.dir_responses = NULL;
    status.skip_paths = NULL;
    status.skip_pos = 0;
    status.replace_paths = NULL;
    status.replace_pos = 0;
    status.keepboth_src_paths = NULL;
    status.keepboth_src_pos = 0;
    status.keepboth_dst_paths = NULL;
    status.keepboth_dst_pos = 0;
    status.errcnt = 0;
    
    int i;
    char *srcs[[theSrcURLs count] + 1];
    char *dsts[[theDstURLs count] + 1];
    
    i = 0;
    for (NSURL *srcURL in theSrcURLs)
        srcs[i++] = (char *)[[srcURL path] fileSystemRepresentation];
    srcs[i] = 0;
    
    i = 0;
    for (NSURL *dstURL in theDstURLs)
        dsts[i++] = (char *)[[dstURL path] fileSystemRepresentation];
    dsts[i] = 0;
    
    int result = preflight_copymove(srcs, dsts, (void *)&status);
    
    if (result == 0)
    {
        status.completed_bytes = 0;
        status.completed_objects = 0;
        status.current_bytes = 0;
        status.start_bytes = 0;
        status.start_time = 0;
        status.dir_resp_cnt = 0;
        status.skip_pos = 0;
        status.replace_pos = 0;
        status.keepboth_src_pos = 0;
        status.keepboth_dst_pos = 0;
        
        copyfile_state_t state = copyfile_state_alloc();
        
        copyfile_state_set(state, COPYFILE_STATE_STATUS_CB, progress_copymove);
        copyfile_state_set(state, COPYFILE_STATE_STATUS_CTX, (void *)&status);
        copyfile_flags_t flags = COPYFILE_ALL | COPYFILE_RECURSIVE | COPYFILE_NOFOLLOW;
        
        for (i = 0; i < [theSrcURLs count]; i++)
        {
            char *src = srcs[i];
            char *dst = dsts[i];
            
            status.src = src;
            status.dst = dst;
            
            result = copyfile(src, dst, state, flags);
            
            if (result != 0)
                break;
        }
        
        copyfile_state_free(state);
        
        if (result == 0 && status.copy && !status.usrcopy && status.skip_pos == 0 && status.errcnt == 0)
            delete(srcs, 0, (void *)&status);
    }
    
    free(status.dir_responses);
    free(status.skip_paths);
    free(status.replace_paths);
    free(status.keepboth_src_paths);
    free(status.keepboth_dst_paths);
    
    if (result == 0)
        return YES;
    else
    {
        if (anError)
        {
            NSURL *failureURL = [NSURL fileURLWithPath:[[NSFileManager defaultManager] stringWithFileSystemRepresentation:status.errpath length:strlen(status.errpath)]];
            free(status.errpath);
            
            NSMutableDictionary *userInfo = [NSMutableDictionary dictionaryWithObjectsAndKeys:
                                             [NSString stringWithUTF8String:strerror(status.err)], NSLocalizedDescriptionKey,
                                             failureURL, NTFileOperationErrorURLKey,
                                             [NSNumber numberWithInt:status.errreason], NTFileOperationErrorReasonKey,
                                             // something for localized version, NSLocalizedFailureReasonErrorKey,
                                             [NSArray arrayWithObjects:@"Skip", @"Quit", nil], NSLocalizedRecoveryOptionsErrorKey, nil];
            
            if (status.err == ERANGE)
                [userInfo setObject:[NSNumber numberWithUnsignedLongLong:status.missing_bytes] forKey:NTFileOperationErrorNeddedSpaceKey];
            
            *anError = [NSError errorWithDomain:NSPOSIXErrorDomain code:status.err userInfo:[NSDictionary dictionaryWithDictionary:userInfo]];
        }
        
        return NO;
    }
}

@end
