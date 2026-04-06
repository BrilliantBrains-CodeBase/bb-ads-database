from fastapi import APIRouter

router = APIRouter(prefix="/claude", tags=["claude"])

# Implemented in Phase 2 Week 7
# POST   /claude/chat                    — streaming SSE response
# POST   /claude/recommendations         — budget recommendations
# POST   /claude/predictions             — multi-step prediction chain
# GET    /claude/conversations           — conversation history
# DELETE /claude/conversations/{id}      — delete conversation
