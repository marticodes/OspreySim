Import(
    rules=[
        'models/base.sml',
        'models/post.sml',
    ]
)

ContainsCat = Rule(
    when_all=[
        EventType == 'create_post',
        TextContains(text=PostText, phrase='cat')
    ],
    description='Post contains the word "cat"',
)

WhenRules(
    rules_any=[ContainsCat],
    then=[BanUser(entity=UserId, comment='User said "cat"')],
)
