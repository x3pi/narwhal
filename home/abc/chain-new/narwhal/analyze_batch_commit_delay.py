import sys

    # Parse command line arguments
    args = sys.argv[1:]
    show_round_timestamps = '--show-round-timestamps' in args
    if show_round_timestamps:
        args = [arg for arg in args if arg != '--show-round-timestamps']
    
    # Parse --top argument trước
