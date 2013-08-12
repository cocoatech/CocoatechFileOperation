//
//  NTFileOperation.h
//  CopyfileTestApp
//
//  Created by Dragan Milić on 29.7.2013.
//  Copyright (c) 2013 Cocoatech. All rights reserved.
//

#import <Foundation/Foundation.h>

typedef enum {
    NTFileOperationStageUndefined = 0,
    NTFileOperationStagePreflighting = 1,
    NTFileOperationStageRunning = 2,
    NTFileOperationStageComplete = 3
} NTFileOperationStage;

typedef enum {
    NTFileConflictResolutionQuit = 1,
    NTFileConflictResolutionSkip,
    NTFileConflictResolutionReplace,
    NTFileConflictResolutionKeepBoth,
    NTFileConflictResolutionMerge
} NTFileConflictResolution;

#define NTFileOperationStageKey @"NTFileOperationStage"
#define NTFileOperationSourceItemKey @"NTFileOperationSourceItem"
#define NTFileOperationDestinationItemKey @"NTFileOperationDestinationItem"
#define NTFileOperationTotalBytesKey @"NTFileOperationTotalBytes"
#define NTFileOperationCompletedBytesKey @"NTFileOperationCompletedBytes"
#define NTFileOperationTotalObjectsKey @"NTFileOperationTotalObjects"
#define NTFileOperationCompletedObjectsKey @"NTFileOperationCompletedObjects"
#define NTFileOperationThroughputKey @"NTFileOperationThroughput"



@class NTFileOperation;

@protocol NTFileOperationDelegate <NSObject>

@optional
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError preflightingItemAtURL:(NSURL *)aSrcURL;
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError copyingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError movingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;

- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictCopyingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL;
- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictMovingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL;

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedOnProgressInfo:(NSDictionary *)theInfo;

@end

@interface NTFileOperation : NSObject
{
    
}

@property (nonatomic, assign) id<NTFileOperationDelegate> delegate;
@property (nonatomic, assign) NSTimeInterval statusChangeInterval;

- (void)copyAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (void)copyAsyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL;
- (BOOL)copySyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL error:(NSError **)anError;
- (BOOL)copySyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL error:(NSError **)anError;

- (void)moveAsyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (void)moveAsyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL;
- (BOOL)moveSyncItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL error:(NSError **)anError;
- (BOOL)moveSyncItemsAtURLs:(NSArray *)theSrcURLs toURL:(NSURL *)aDstURL error:(NSError **)anError;

@end
