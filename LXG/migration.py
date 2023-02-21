class Migration:
    def __init__(self, name):
        self.str = name

    def __repr__(self):
        return f"Hello,{self.str} this is __repr__ part of Migration class"

    def __str__(self):
        return f"Hello,{self.str} this is __str__ part of Migration class"
