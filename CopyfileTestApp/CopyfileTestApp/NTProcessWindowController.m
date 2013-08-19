//
//  NTProcessWindowController.m
//  CopyfileTestApp
//
//  Created by Dragan Milić on 11.8.2013.
//  Copyright (c) 2013 Cocoatech. All rights reserved.
//

#import "NTProcessWindowController.h"
#import "NTFileOperation.h"

@interface NTProcessWindowController () <NTFileOperationDelegate>

@property (nonatomic, retain) IBOutlet NSTextField *sourceTextField;
@property (nonatomic, retain) IBOutlet NSTextField *destinationTextField;
@property (nonatomic, retain) IBOutlet NSTextField *infoTextField;
@property (nonatomic, retain) IBOutlet NSProgressIndicator *progressIndicator;
@property (nonatomic, assign) BOOL stopped;

@end

@interface NTProcessWindowController (Private)

- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictProcessingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL isCopy:(BOOL)isCopy;
- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError processingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL;
- (NSString *)byteStringForBytes:(unsigned long long)theBytes;

@end

@implementation NTProcessWindowController

- (void)dealloc
{
    [self setSourceTextField:nil];
    [self setDestinationTextField:nil];
    [self setInfoTextField:nil];
    [self setProgressIndicator:nil];
    
    [super dealloc];
}

+ (void)showWindowForOperation:(NTFileOperation *)anOperation
{
    NTProcessWindowController *controller = [[NTProcessWindowController alloc] init];
    
    [anOperation setDelegate:controller];
    
    [controller setStopped:NO];
    [[controller window] makeKeyAndOrderFront:self];
}

- (id)init
{
    self = [super initWithWindowNibName:@"NTProcessWindow" owner:self];
    
    return self;
}

- (void)stop:(id)sender
{
    [self setStopped:YES];
}

- (void)windowWillClose:(NSNotification *)aNotification
{
    [self autorelease];
}

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedOnProgressInfo:(NSDictionary *)theInfo
{
    unsigned long long completedBytes = [[theInfo objectForKey:NTFileOperationCompletedBytesKey] unsignedLongLongValue];
    unsigned long long totalBytes = [[theInfo objectForKey:NTFileOperationTotalBytesKey] unsignedLongLongValue];
    unsigned long long completedObjects = [[theInfo objectForKey:NTFileOperationCompletedObjectsKey] unsignedLongLongValue];
    unsigned long long totalObjects = [[theInfo objectForKey:NTFileOperationTotalObjectsKey] unsignedLongLongValue];
    unsigned long long throughput = [[theInfo objectForKey:NTFileOperationThroughputKey] unsignedLongLongValue];
    unsigned long long secondsRemaining = (throughput == 0) ? 0 : (totalBytes - completedBytes) / throughput;
    
    NTFileOperationStage stage = (NTFileOperationStage)[[theInfo objectForKey:NTFileOperationStageKey] unsignedIntegerValue];
    
    NSString *sourceText = [NSString stringWithFormat:@"Source: %@", [[theInfo objectForKey:NTFileOperationSourceItemKey] path]];
    NSString *destinationText = [NSString stringWithFormat:@"Destination: %@", [[theInfo objectForKey:NTFileOperationDestinationItemKey] path]];
    NSString *infoText = @"";
    
    if (stage == NTFileOperationStagePreflighting)
        infoText = [NSString stringWithFormat:@"Preparing to process %llu items.", totalObjects];
    else if (stage == NTFileOperationStageRunning)
        infoText = [NSString stringWithFormat:@"Processing %@ of %@ (%llu of %llu items). Speed %@/s, %llu sec. remainig.", [self byteStringForBytes:completedBytes], [self byteStringForBytes:totalBytes], completedObjects, totalObjects, [self byteStringForBytes:throughput], secondsRemaining];
    
    dispatch_async(dispatch_get_main_queue(), ^
    {
        [[self infoTextField] setStringValue:infoText];
        
        if (stage == NTFileOperationStagePreflighting)
        {
            [[self progressIndicator] startAnimation:self];
            
            [[self sourceTextField] setStringValue:sourceText];
        }
        else if (stage == NTFileOperationStageRunning)
        {
            if ([[self progressIndicator] isIndeterminate])
            {
                [[self progressIndicator] stopAnimation:self];
                [[self progressIndicator] setIndeterminate:NO];
            }
            
            [[self progressIndicator] setDoubleValue:((double)completedBytes / (double)totalBytes) * 100.0];
            
            [[self sourceTextField] setStringValue:sourceText];
            [[self destinationTextField] setStringValue:destinationText];
        }
        else if (stage == NTFileOperationStageComplete)
        {
            [[self progressIndicator] stopAnimation:self];
            [self close];
        }
    });
    
    return ![self stopped];
}

- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictCopyingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL
{
    return [self fileOperation:anOperation conflictProcessingItemAtURL:aSrcURL toURL:aDstURL proposedURL:aPropURL isCopy:YES];
}

- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictMovingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL
{
    return [self fileOperation:anOperation conflictProcessingItemAtURL:aSrcURL toURL:aDstURL proposedURL:aPropURL isCopy:NO];
}

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError preflightingItemAtURL:(NSURL *)aSrcURL
{
    return [self fileOperation:anOperation shouldProceedAfterError:anError processingItemAtURL:aSrcURL toURL:nil];
}

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError copyingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL
{
    return [self fileOperation:anOperation shouldProceedAfterError:anError processingItemAtURL:aSrcURL toURL:aDstURL];
}

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError movingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL
{
    return [self fileOperation:anOperation shouldProceedAfterError:anError processingItemAtURL:aSrcURL toURL:aDstURL];
}

- (void)conflictAlertDidEnd:(NSAlert *)anAlert returnCode:(NSInteger)aReturnCode contextInfo:(void *)aContextInfo
{
    NTFileConflictResolution *result = (NTFileConflictResolution *)aContextInfo;
    BOOL areDirs = [[anAlert buttons] count] == 5;
    
    if (areDirs)
    {
        switch (aReturnCode)
        {
            case NSAlertSecondButtonReturn:
                *result = NTFileConflictResolutionKeepBoth;
                break;
            case NSAlertThirdButtonReturn:
                *result = NTFileConflictResolutionReplace;
                break;
            case NSAlertThirdButtonReturn + 1:
                *result = NTFileConflictResolutionSkip;
                break;
            case NSAlertThirdButtonReturn + 2:
                *result = NTFileConflictResolutionQuit;
                break;
            case NSAlertFirstButtonReturn:
            default:
                *result = NTFileConflictResolutionMerge;
                break;
        }
    }
    else
    {
        switch (aReturnCode)
        {
            case NSAlertSecondButtonReturn:
                *result = NTFileConflictResolutionReplace;
                break;
            case NSAlertThirdButtonReturn:
                *result = NTFileConflictResolutionSkip;
                break;
            case NSAlertThirdButtonReturn + 1:
                *result = NTFileConflictResolutionQuit;
                break;
            case NSAlertFirstButtonReturn:
            default:
                *result = NTFileConflictResolutionKeepBoth;
                break;
        }
    }
}

- (void)errorAlertDidEnd:(NSAlert *)anAlert returnCode:(NSInteger)aReturnCode contextInfo:(void *)aContextInfo
{
    int *result = (int *)aContextInfo;
    
    switch (aReturnCode)
    {
        case NSAlertSecondButtonReturn:
            *result = 0;
            break;
        case NSAlertFirstButtonReturn:
        default:
            *result = 1;
            break;
    }
}

- (NTFileConflictResolution)fileOperation:(NTFileOperation *)anOperation conflictProcessingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL proposedURL:(NSURL **)aPropURL isCopy:(BOOL)isCopy
{
    __block NTFileConflictResolution result = -1;
    
    NSArray *keys = [NSArray arrayWithObjects:NSURLIsDirectoryKey, NSURLIsPackageKey, nil];
    
    NSDictionary *srcProperties = [aSrcURL resourceValuesForKeys:keys error:NULL];
    NSDictionary *dstProperties = [aDstURL resourceValuesForKeys:keys error:NULL];
    
    BOOL isSrcDir = [[srcProperties objectForKey:NSURLIsDirectoryKey] boolValue] && ![[srcProperties objectForKey:NSURLIsPackageKey] boolValue];
    BOOL isDstDir = [[dstProperties objectForKey:NSURLIsDirectoryKey] boolValue] && ![[dstProperties objectForKey:NSURLIsPackageKey] boolValue];
    
    BOOL areDirs = isSrcDir && isDstDir;
    
    NSString *firstButton;
    NSString *secondButton;
    NSString *thirdButton;
    NSString *fourthButton;
    
    if (areDirs)
    {
        firstButton = @"Merge";
        secondButton = @"Keep both";
        thirdButton = @"Replace";
        fourthButton = @"Skip";
    }
    else
    {
        firstButton = @"Keep both";
        secondButton = @"Replace";
        thirdButton = @"Skip";
        fourthButton = @"Stop";
    }
    
    NSAlert *alert = [[[NSAlert alloc] init] autorelease];
    
    [alert setMessageText:[NSString stringWithFormat:@"%@ already exists!", (isDstDir ? @"Folder" : @"File")]];
    
    if (isCopy)
        [alert setInformativeText:[NSString stringWithFormat:@"You want to copy %@ \"%@\" but designated %@ \"%@\" already exists. Proposed new path is \"%@\". What do you want to do?", (isSrcDir ? @"folder" : @"file"), [aSrcURL path], (isDstDir ? @"folder" : @"file"), [aDstURL path], [*aPropURL path]]];
    else
        [alert setInformativeText:[NSString stringWithFormat:@"You want to move %@ \"%@\" but designated %@ \"%@\" already exists. Proposed new path is \"%@\". What do you want to do?", (isSrcDir ? @"folder" : @"file"), [aSrcURL path], (isDstDir ? @"folder" : @"file"), [aDstURL path], [*aPropURL path]]];
    
    [alert addButtonWithTitle:firstButton];
    [alert addButtonWithTitle:secondButton];
    [alert addButtonWithTitle:thirdButton];
    [alert addButtonWithTitle:fourthButton];
    
    if (areDirs)
        [alert addButtonWithTitle:@"Stop"];
    
    dispatch_async(dispatch_get_main_queue(), ^
    {
        [alert beginSheetModalForWindow:[self window] modalDelegate:self didEndSelector:@selector(conflictAlertDidEnd:returnCode:contextInfo:) contextInfo:&result];
    });
    
    while (result == -1);
    
    return result;
}

- (BOOL)fileOperation:(NTFileOperation *)anOperation shouldProceedAfterError:(NSError *)anError processingItemAtURL:(NSURL *)aSrcURL toURL:(NSURL *)aDstURL
{
    __block int result = -1;
    
    NSAlert *alert = [NSAlert alertWithError:anError];
    [alert setInformativeText:[NSString stringWithFormat:@"%@", [anError localizedFailureReason]]];
    
    dispatch_async(dispatch_get_main_queue(), ^
    {
        [alert beginSheetModalForWindow:[self window] modalDelegate:self didEndSelector:@selector(errorAlertDidEnd:returnCode:contextInfo:) contextInfo:&result];
    });
    
    while (result == -1);
    
    return (BOOL)result;
}

- (NSString *)byteStringForBytes:(unsigned long long)theBytes
{
    NSString *result = @"";
    
    if (theBytes < 1000)
        result = [NSString stringWithFormat:@"%llu bytes", theBytes];
    else if (theBytes < 1000000)
        result = [NSString stringWithFormat:@"%.2f KB", (float)theBytes / 1000];
    else if (theBytes < 1000000000)
        result = [NSString stringWithFormat:@"%.2f MB", (float)theBytes / 1000000];
    else
        result = [NSString stringWithFormat:@"%.2f GB", (float)theBytes / 1000000000];
    
    return result;
}

@end
