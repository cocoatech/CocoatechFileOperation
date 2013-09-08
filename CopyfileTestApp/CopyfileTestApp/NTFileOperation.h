//
//  NTFileOperation.h
//  CopyfileTestApp
//
//  Created by Dragan Milić on 29.7.2013.
//  Copyright (c) 2013 Cocoatech. All rights reserved.
//

#import <Foundation/Foundation.h>

enum {
    NTFileOperationDefaultOptions = 0x0,
    NTFileOperationOverwrite = 0x1,
    NTFileOperationSkipPermissionErrors = 0x2,
    NTFileOperationDoNotMoveAcrossVolumes = 0x4
};

typedef enum {
    NTFileOperationTypeUndefined = 0,
    NTFileOperationTypeCopy,
    NTFileOperationTypeMove,
    NTFileOperationTypeDelete
} NTFileOperationType;

typedef enum {
    NTFileOperationStageUndefined = 0,
    NTFileOperationStagePreflighting,
    NTFileOperationStageRunning,
    NTFileOperationStageComplete
} NTFileOperationStage;

typedef enum {
    NTFileConflictResolutionQuit = 1,
    NTFileConflictResolutionSkip,
    NTFileConflictResolutionReplace,
    NTFileConflictResolutionKeepBoth,
    NTFileConflictResolutionMerge
} NTFileConflictResolution;

typedef enum {
    NTFileErrorReasonOther = 0,
    NTFileErrorReasonItemNotReadable,
    NTFileErrorReasonItemNotWritable,
    NTFileErrorReasonItemNotMovable,
    NTFileErrorReasonItemNotDeletable,
    NTFileErrorReasonItemLocked,
    NTFileErrorReasonNoFreeSpace
} NTFileErrorReason;

#define NTFileOperationTypeKey @"NTFileOperationType"
#define NTFileOperationStageKey @"NTFileOperationStage"
#define NTFileOperationSourcePathKey @"NTFileOperationSourcePath"
#define NTFileOperationDestinationPathKey @"NTFileOperationDestinationPath"
#define NTFileOperationSourceItemPathKey @"NTFileOperationSourceItemPath"
#define NTFileOperationDestinationItemPathKey @"NTFileOperationDestinationPathItem"
#define NTFileOperationTotalBytesKey @"NTFileOperationTotalBytes"
#define NTFileOperationCompletedBytesKey @"NTFileOperationCompletedBytes"
#define NTFileOperationTotalObjectsKey @"NTFileOperationTotalObjects"
#define NTFileOperationCompletedObjectsKey @"NTFileOperationCompletedObjects"
#define NTFileOperationThroughputKey @"NTFileOperationThroughput"
#define NTFileOperationErrorURLKey @"NTFileOperationErrorURL"
#define NTFileOperationErrorReasonKey @"NTFileOperationErrorReason"
#define NTFileOperationErrorNeddedSpaceKey @"NTFileOperationErrorNeddedSpace"



@class NTFileOperation;

@protocol NTFileOperationDelegate <NSObject>

@optional
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError preflightingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError copyingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError movingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError deletingItemAtURL:(NSURL *)aSrcURL;

- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictCopyingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL;
- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictMovingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL;

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedOnProgressInfo:(NSDictionary *)theInfo;

@end

@interface NTFileOperation : NSObject
{
    
}

@property (nonatomic, assign) id<NTFileOperationDelegate> delegate;
@property (nonatomic, assign) NSTimeInterval statusChangeInterval;

- (void)copyAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask;
- (void)copyAsyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask;
- (BOOL)copySyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask error:(NSError **)anError;
- (BOOL)copySyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURLs options:(NSUInteger)anOptMask error:(NSError **)anError;

- (void)moveAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask;
- (void)moveAsyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURL options:(NSUInteger)anOptMask;
- (BOOL)moveSyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL options:(NSUInteger)anOptMask error:(NSError **)anError;
- (BOOL)moveSyncItemsAtURLs:(NSArray *)theSrcURLs toURLs:(NSArray *)theDstURL options:(NSUInteger)anOptMask error:(NSError **)anError;

- (void)deleteAsyncItemAtURL:(NSURL *)aSrcURL options:(NSUInteger)anOptMask;
- (void)deleteAsyncItemsAtURLs:(NSArray *)theSrcURLs options:(NSUInteger)anOptMask;
- (BOOL)deleteSyncItemAtURL:(NSURL *)aSrcURL options:(NSUInteger)anOptMask error:(NSError **)anError;
- (BOOL)deleteSyncItemsAtURLs:(NSArray *)theSrcURLs options:(NSUInteger)anOptMask error:(NSError **)anError;



@end
