//
//  NTAppDelegate.m
//  CopyfileTestApp
//
//  Created by Dragan Milić on 25.7.2013.
//  Copyright (c) 2013 Cocoatech. All rights reserved.
//

#import "NTAppDelegate.h"
#import "NTFileOperation.h"
#import "NTProcessWindowController.h"

@interface NTAppDelegate () <NTFileOperationDelegate>

@property (nonatomic, retain) IBOutlet NSWindow *window;

@property (nonatomic, assign) BOOL isCopy;
@property (nonatomic, assign) dispatch_queue_t operationQueue;
@property (nonatomic, retain) NSMutableDictionary *directoryResponses;

@end

@implementation NTAppDelegate

- (void)dealloc
{
    [self setWindow:nil];
    
    [super dealloc];
}

- (void)awakeFromNib
{
    [[self window] center];
}

- (void)doStuff:(id)aSender
{
    NSOpenPanel *sourcePanel = [NSOpenPanel openPanel];
    
    [sourcePanel setCanChooseDirectories:YES];
    [sourcePanel setAllowsMultipleSelection:YES];
    [sourcePanel setResolvesAliases:NO];
    [sourcePanel setTitle:@"Source?"];
    [sourcePanel setPrompt:@"Choose"];
    
    NSInteger result = [sourcePanel runModal];
    
    if (result == NSFileHandlingPanelOKButton)
    {
        BOOL isCopy = ([aSender tag] == 0);
        NSOpenPanel *destinationPanel = [NSOpenPanel openPanel];
        
        [destinationPanel setCanChooseDirectories:YES];
        [destinationPanel setAllowsMultipleSelection:NO];
        [destinationPanel setCanChooseFiles:NO];
        [destinationPanel setTitle:@"Destination?"];
        [destinationPanel setPrompt:isCopy ? @"Copy" : @"Move"];
        
        result = [destinationPanel runModal];
        
        if (result == NSFileHandlingPanelOKButton)
        {
            NTFileOperation *fileOperation = [[[NTFileOperation alloc] init] autorelease];
            
            [NTProcessWindowController showWindowForOperation:fileOperation];
            
            dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^
            {
                if (isCopy)
                    [fileOperation copyAsyncItemsAtURLs:[sourcePanel URLs] toURL:[destinationPanel URL]];
                else
                    [fileOperation moveAsyncItemsAtURLs:[sourcePanel URLs] toURL:[destinationPanel URL]];
            });
        }
    }
}

@end
