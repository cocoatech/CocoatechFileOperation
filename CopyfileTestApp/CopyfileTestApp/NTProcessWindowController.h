//
//  NTProcessWindowController.h
//  CopyfileTestApp
//
//  Created by Dragan Milić on 11.8.2013.
//  Copyright (c) 2013 Cocoatech. All rights reserved.
//

#import <Cocoa/Cocoa.h>

@class NTFileOperation;

@interface NTProcessWindowController : NSWindowController
{
    
}

+ (void)showWindowForOperation:(NTFileOperation *)anOperation;

@end
